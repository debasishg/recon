package net.debasishg.recon

import com.twitter._
import util.{Future, FuturePool, Return, TimeoutException, Timer, JavaTimer}
import conversions.time._
import com.redis._
import serialization._
import java.util.concurrent.Executors

import scalaz._
import Scalaz._
import Util._

trait ReconEngine { 

  implicit val timer = new JavaTimer

  // set up Executors
  lazy val futures = FuturePool(Executors.newFixedThreadPool(8))

  /**
   * tolerance function for comparing values
   * Can be overridden in making concrete Recon Engines. The default implementation is based on
   * equality.
   * @todo Explore if scalaz.Equal can be used
   */
  type X
  def tolerancefn(x: X, y: X)(implicit ex: Equal[X]): Boolean = x === y

  def loadOneReconSet[T, V](defn: ReconDef[T])
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
      val id = defn.id

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

  def loadReconInputData[T, V](ds: Seq[ReconDef[T]])(implicit clients: RedisClientPool, parse: Parse[V], m: Monoid[V], p: ReconProtocol[T, V], mv: Manifest[V]): Seq[Either[Throwable, String]] = {
    val fs =
      ds.map {d =>
        futures {
          Right(loadOneReconSet(d))
        }.within(120.seconds) handle {
           case x: TimeoutException => Left(x)
        }
      }
    Future.collect(fs.toSeq) apply
  }

  def recon[V <: X](ids: Seq[String], 
    matchFn: (MatchList[V], (V, V) => Boolean) => MatchFunctions.ReconRez)
    (implicit clients: RedisClientPool, parsev: Parse[V], m: Monoid[V], ex: Equal[X]) = {

    val fields = clients.withClient {client =>
      ids.map(client.hkeys[String](_)).view.flatten.flatten.toSet 
    }

    fields.par.map {field => 
      clients.withClient {client =>
        import client._
        val maps: Seq[Option[List[V]]] = ids.map {id =>
          val hk = hget[String](id, field)
          hk match {
            case Some(s) => hgetall[String, V](s).map(_.values).map(_.toList)
            case None => none[List[V]]
          }
        }
        ReconResult(field, maps.toList, matchFn(maps.toList, tolerancefn))
      }
    }
  }
}

