package recon

import com.twitter._
import util.{Future, FuturePool, Return, TimeoutException, Timer, JavaTimer}
import conversions.time._
import com.redis._
import serialization._
import java.util.concurrent.Executors

import scalaz._
import Scalaz._

trait ReconEngine { 
  type ReconId 

  implicit val timer = new JavaTimer

  // set up Executors
  val futures = FuturePool(Executors.newFixedThreadPool(8))

  def loadOneReconSet[T, K, V](defn: ReconDef[ReconId, T])(implicit clients: RedisClientPool, format: Format, parse: Parse[V], m: Monoid[V], p: ReconProtocol[T, K, V]) = clients.withClient {client =>
    import client._

    def load(value: T) {
      val gk = p.groupKey(value)
      val mv = p.matchValue(value)
      val id = defn.id

      hsetnx(id, gk, mv) unless { // get on Option is safe since hsetnx has returned false
        hset(id, gk, m append (hget[V](id, gk).get, mv)) 
      }
    }

    // ugly, but don't want the guard check every time for no-predicate scenario
    if (!defn.maybePred.isDefined) 
      defn.values.foreach(load(_)) 
    else 
      for(v <- defn.values if defn.maybePred.get(v) == true) load(v)

    hlen(id)
  }

  def loadReconInputData[T, K, V](ds: Seq[ReconDef[ReconId, T]])(implicit clients: RedisClientPool, parse: Parse[V], m: Monoid[V], p: ReconProtocol[T, K, V]): Seq[Option[Int]] = {
    val fs =
      ds.map {d =>
        futures {
          loadOneReconSet(d)
        }.within(120.seconds) handle {
          case _: TimeoutException => None
        }
      }
    Future.collect(fs.toSeq) apply
  }

  def recon[K, V](ids: Seq[ReconId], fn: List[Option[V]] => Boolean)(implicit clients: RedisClientPool, parsev: Parse[V], parsek: Parse[K], m: Monoid[V]) = {
    val fields = clients.withClient {client =>
      ids.map(client.hkeys[K](_)).view.flatten.flatten.toSet 
    }

    fields.par.map {field => 
      clients.withClient {client =>
        (field, fn(ids.map {id => client.hget[V](id, field)}.toList))
      }
    }
  }
}
