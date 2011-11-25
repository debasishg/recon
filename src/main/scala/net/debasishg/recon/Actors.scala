package net.debasishg.recon

import com.redis._
import MatchFunctions._

import sjson.json.DefaultProtocol._
import Util._
import com.redis.serialization._

import scalaz._
import Scalaz._

import akka.actor.{Actor, ActorRef}
import akka.camel.{Message, Consumer}

abstract class ReconConsumer[T, V](engine: ReconEngine[T, V], loader: ActorRef)
  (implicit clients: RedisClientPool, 
            parse: Parse[V], 
            m: Monoid[V], 
            ex: Equal[V], 
            p: Parse[MatchList[V]], 
            f: Format) extends Actor with Consumer {

  def endpointUri: String

  def getSourceConfig(file: String): ReconSource[T]

  def receive = {
    case msg: Message => 
      val fileName = msg.getHeaderAs("CamelFilePath", classOf[String])
      loader forward msg.transformBody((m: java.io.InputStream) => 
        engine.fromSource((fileName, getSourceConfig(fileName))))
  }
}

class ReconLoader[T, V](engine: ReconEngine[T, V], processor: ActorRef)
  (implicit clients: RedisClientPool, 
            format: Format, 
            parse: Parse[V], 
            m: Monoid[V], 
            p: ReconProtocol[T, V], 
            mv: Manifest[V]) extends Actor {

  protected def receive = {
    case Message(Some(rdef), _) => processor forward (engine.loadOneReconSet(rdef.asInstanceOf[ReconDef[T]]))
    case _ => sys.error("error")
  }
}

class ReconProcessor[T, V](engine: ReconEngine[T, V], completionPredicate: (List[String]) => Boolean)
  (implicit clients: RedisClientPool, 
            parse: Parse[V], 
            m: Monoid[V], 
            ex: Equal[V], 
            p: Parse[MatchList[V]], 
            f: Format) extends Actor {

  val ids = collection.mutable.ListBuffer.empty[String]
  def receive = {
    case id: String =>
      ids append id.asInstanceOf[String]
      if (completionPredicate(ids.toList)) {
        val res = engine.reconcile(ids.toList, matchHeadAsSumOfRest).seq
        println(engine.persist(res))
      }
    case _ => sys.error("error")
  }
}
