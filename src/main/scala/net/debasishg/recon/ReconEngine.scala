package net.debasishg.recon

import com.twitter._
import util.{Future, FuturePool, Return, TimeoutException, Timer, JavaTimer}
import conversions.time._
import com.redis._
import serialization._
import java.util.concurrent.Executors
import org.joda.time.LocalDate

import scalaz._
import Scalaz._
import Util._
import MatchFunctions._

trait ReconEngine { 

  implicit val timer = new JavaTimer

  // set up Executors
  lazy val futures = FuturePool(Executors.newFixedThreadPool(8))
  def clientName: String
  def runDate: LocalDate

  case class Pkey(id: String) {
    override def toString = id + ":" + runDate
  }

  /** @todo check error handling for I/O error **/
  def fromSource[A](fs: Seq[(String, ReconSource[A])]): Option[Seq[ReconDef[A]]] = {
    try {
      val pr = fs.par.map {case(file, src) => 
        CollectionDef(src.id + runDate.toString, src.process(file).flatten.flatten)
      }
      some(pr.seq.toSeq)
    } catch {case e => none}
  }

  /**
   * tolerance function for comparing values
   * Can be overridden in making concrete Recon Engines. The default implementation is based on
   * equality.
   * @todo Explore if scalaz.Equal can be used
   */
  type X
  def tolerancefn(x: X, y: X)(implicit ex: Equal[X]): Boolean = x === y

  private[this] def loadOneReconSet[T, V](defn: ReconDef[T])
    (implicit clients: RedisClientPool, format: Format, parse: Parse[V], m: Monoid[V], 
     p: ReconProtocol[T, V], mv: Manifest[V]): String = clients.withClient {client =>
    import client._

    /**
     * Using 2 levels of hash. The first level stores the recon id as the key, the group key as the
     * field and key for the second level hash as the value. e.g.
     *
     *   +-----------------------------+           +---------------------------------------+
     *   | r1 | account120111212 |  x--|---------> | r1:account120111212 | quantity | 200  |
     *   +------------+----------------+           +---------------------+----------+------+
     *                |                                                 | amount    | 2000 |
     *                |                                                 +-----------+------+
     *        +-------+----------------+           +---------------------------------------+
     *        | account220111212 |  x--|---------> | r1:account220111212 | quantity | 100  |
     *        +------------------------+           +---------------------+----------+------+
     *                                                                  | amount    | 2500 |
     *                                                                  +-----------+------+
     *
     */                                                                  
    def load(value: T) {
      val gk = p.groupKey(value)
      val mvs = p.matchValues(value)
      val id = Pkey(defn.id).toString

      // group key + id = key to the second level hash
      val mapId = gk + ":" + id.toString

      // level 1 hash population
      hsetnx(id, gk, mapId)
      mvs.map {case (k, v) => 
        hsetnx(mapId, k, v) unless { // level 2 hash population
          hset(mapId, k, hget[V](mapId, k).get |+| v) 
        }
      }
    }

    // ugly, but don't want the guard check every time for no-predicate scenario
    if (!defn.maybePred.isDefined) 
      defn.values.foreach(load(_)) 
    else 
      for(v <- defn.values if defn.maybePred.get(v) == true) load(v)

    defn.id
  }

  def loadInput[T, V](ds: Seq[ReconDef[T]])(implicit clients: RedisClientPool, parse: Parse[V], m: Monoid[V], p: ReconProtocol[T, V], mv: Manifest[V]): Seq[Either[Throwable, String]] = {
    val fs =
      ds.map {d =>
        futures {
          Right(loadOneReconSet(d))
        }.within(300.seconds) handle {
           case x: TimeoutException => Left(x)
        }
      }
    Future.collect(fs.toSeq) apply
  }

  def reconcile[V <: X](ids: Seq[String], 
    matchFn: (MatchList[V], (V, V) => Boolean) => MatchFunctions.ReconRez)
    (implicit clients: RedisClientPool, parsev: Parse[V], m: Monoid[V], ex: Equal[X]) = {

    val fields = clients.withClient {client =>
      ids.map(id => client.hkeys[String](Pkey(id).toString)).view.flatten.flatten.toSet 
    }

    fields.par.map {field => 
      clients.withClient {client =>
        import client._
        val maps: Seq[Option[List[V]]] = ids.map {id =>
          val hk = hget[String](Pkey(id).toString, field)
          hk match {
            case Some(s) => hgetall[String, V](s).map(_.values).map(_.toList)
            case None => none[List[V]]
          }
        }
        ReconResult(field, maps.toList, matchFn(maps.toList, tolerancefn))
      }
    }
  }

  def persist[V <: X](rs: Set[ReconResult[V]])
    (implicit clients: RedisClientPool, m: Monoid[V], p: Parse[MatchList[V]], f: Format) = {

    val matchHashKey = clientName + ":" + runDate + ":" + Match
    val breakHashKey = clientName + ":" + runDate + ":" + Break
    val unmatchHashKey = clientName + ":" + runDate + ":" + Unmatch

    clients.withClient {client =>
      import client._
      rs map {r =>
        r.result match {
          case Match => hset(matchHashKey, r.field, r.matched)
          case Break => hset(breakHashKey, r.field, r.matched)
          case Unmatch => hset(unmatchHashKey, r.field, r.matched)
        }
      }
      Map(Match -> (hgetall[String, MatchList[V]](matchHashKey).map(_.keySet.size)),
          Break -> (hgetall[String, MatchList[V]](breakHashKey).map(_.keySet.size)),
          Unmatch -> (hgetall[String, MatchList[V]](unmatchHashKey).map(_.keySet.size)))
    }
  }

  def consolidateWith[V <: X](pastDate: LocalDate) (implicit clients: RedisClientPool, m: Monoid[V], s: Semigroup[MatchList[V]], p: Parse[MatchList[V]], f: Format) = {

    val keyFormat = clientName + ":%s:%s"
    val breakKey = keyFormat.format(pastDate, Break)
    val currBreakKey = keyFormat.format(now, Break)
    val unmatchKey = keyFormat.format(pastDate, Unmatch)
    val currUnmatchKey = keyFormat.format(now, Unmatch)

    clients.withClient {client =>
      import client._

      // get past breaks
      val breaks = hgetall[String, MatchList[V]](breakKey)

      // consolidate with current breaks
      breaks foreach {b => 
        b map {case (f, v) => // f -> field of the prev day
          hget[MatchList[V]](currBreakKey, f) map {ml => 
            hset(currBreakKey, f, ml |+| v)
          }
        }
      }

      // get past unmatches
      val unmatches = hgetall[String, MatchList[V]](unmatchKey)

      // consolidate with current unmatches
      unmatches foreach {b => 
        b map {case (f, v) => // f -> field of the prev day
          hget[MatchList[V]](currUnmatchKey, f) map {ml => 
            hset(currUnmatchKey, f, ml |+| v)
          }
        }
      }
    }
  }
}

