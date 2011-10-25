package recon

import com.twitter._
import util.{Future, FuturePool, Return, TimeoutException, Timer, JavaTimer}
import conversions.time._
import com.redis._
import serialization._
import java.util.concurrent.Executors

import scalaz._
import Scalaz._

object ReconEngine {
  type ReconId = String

  implicit val timer = new JavaTimer

  // set up Executors
  val futures = FuturePool(Executors.newFixedThreadPool(8))

  def loadOneReconSet[T, K, V](id: ReconId, values: Seq[T])(implicit clients: RedisClientPool, format: Format, parse: Parse[V], m: Monoid[V], p: ReconProtocol[T, K, V]) = clients.withClient {client =>
    import client._
    values.foreach {value => 
      val gk = p.groupKey(value)
      val mv = p.matchValue(value)
      hsetnx(id, gk, mv) unless {
        hset(id, gk, m append (hget[V](id, gk).get, mv)) // get on Option is safe since hsetnx has returned false
      }
    }
    hlen(id)
  }

  def loadReconInputData[T, K, V](ds: Map[ReconId, Seq[T]])(implicit clients: RedisClientPool, parse: Parse[V], m: Monoid[V], p: ReconProtocol[T, K, V]) = {
    val fs =
      ds.map {case (id, bs) =>
        futures {
          loadOneReconSet(id, bs)
        }.within(120.seconds) handle {
          case _: TimeoutException => None
        }
      }
    val all = Future.collect(fs.toSeq)
    all.apply
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
