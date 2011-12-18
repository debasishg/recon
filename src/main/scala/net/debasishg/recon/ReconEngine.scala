package net.debasishg.recon

import util.control.Breaks._
import collection.mutable.ListBuffer

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

trait ReconEngine[T, V] { 

  implicit val timer = new JavaTimer
  type EitherEx[A] = Either[Throwable, A]

  // set up Executors
  lazy val futures = FuturePool(Executors.newFixedThreadPool(8))
  def clientName: String
  def runDate: LocalDate

  case class Pkey(id: String) {
    override def toString = id + ":" + runDate
  }

  // uses breakable as an optimization strategy
  // break as soon as you get an exception processing an input file
  // failfast is the right strategy for recon
  def fromSource(fs: Seq[(String, ReconSource[T])]): Option[Seq[ReconDef[T]]] = {
    var ex: Throwable = null
    val list: ListBuffer[ReconDef[T]] = ListBuffer.empty
    breakable {
      fs.foreach {case(file, src) => 
        import src._
        val a = process(file) :-> (x => CollectionDef(id + runDate.toString, x.flatten.flatten))
        a match {
          case Left(x) => ex = x; x.printStackTrace; break
          case Right(y) => list += y
        }
      }
    }
    if (ex != null) none else list.toSeq.some
  }

  def fromSource(fs: (String, ReconSource[T])): Option[ReconDef[T]] = {
    val (file, src) = fs
    import src._
    val a = process(file) :-> (x => CollectionDef(id + runDate.toString, x.flatten.flatten))
    a match {
      case Left(x) => none
      case Right(y) => y.some
    }
  }

  /**
   * tolerance function for comparing values
   * Can be overridden in making concrete Recon Engines. The default implementation is based on
   * equality.
   * @todo Explore if scalaz.Equal can be used
   */
  type X
  def tolerancefn(x: V, y: V)(implicit ex: Equal[V]): Boolean = x === y

  def loadOneReconSet(defn: ReconDef[T])
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

  def loadInput(ds: Seq[ReconDef[T]])(implicit clients: RedisClientPool, parse: Parse[V], m: Monoid[V], p: ReconProtocol[T, V], mv: Manifest[V]): Either[Throwable, Seq[String]] = {
    val fs =
      ds.map {d =>
        futures {
          Right(loadOneReconSet(d))
        }.within(300.seconds) handle {
           case x: TimeoutException => Left(x)
        }
      }
    (Future.collect(fs.toSeq) apply).sequence[EitherEx, String]
  }

  def reconcile(ids: Seq[String], 
    matchFn: (MatchList[V], (V, V) => Boolean) => MatchFunctions.ReconRez)
    (implicit clients: RedisClientPool, parsev: Parse[V], m: Monoid[V], ex: Equal[V]) = {

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

  def persist(rs: Set[ReconResult[V]])
    (implicit clients: RedisClientPool, m: Monoid[V], p: Parse[MatchList[V]], f: Format) = {

    val matchHashKey = clientName + ":" + runDate + ":" + Match
    val breakHashKey = clientName + ":" + runDate + ":" + Break
    val unmatchHashKey = clientName + ":" + runDate + ":" + Unmatch

    val res = 
    clients.withClient {client =>
      client.pipeline {pc =>
        import pc._
        try {
          rs map {r =>
            r.result match {
              case Match => hset(matchHashKey, r.field, r.matched)
              case Break => hset(breakHashKey, r.field, r.matched)
              case Unmatch => hset(unmatchHashKey, r.field, r.matched)
            }
          }
        } catch { 
          case th: Throwable => 
            throw RedisMultiExecException(th.getMessage)  // need to log exception too
        }
      }
    }
    res.size
  }

  def consolidateWith(pastDate: LocalDate) (implicit clients: RedisClientPool, m: Monoid[V], s: Semigroup[MatchList[V]], p: Parse[MatchList[V]], f: Format) = {

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

