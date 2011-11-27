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
import akka.routing.{Routing, CyclicIterator}
import akka.dispatch.Dispatchers
import akka.actor.Actor._

object ReconActors {

  val reconDispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("recon-dispatcher")
    .withNewThreadPoolWithLinkedBlockingQueueWithCapacity(100)
    .setCorePoolSize(20)
    .build

  private def loadBalanced(poolSize: Int, actor: ⇒ ActorRef): ActorRef = {
    val workers = Vector.fill(poolSize)(actor.start())
    Routing.loadBalancerActor(CyclicIterator(workers)).start()
  }

  // sets up the pool and starts the worker actors
  def fileTransformationService[T, V](engine: ReconEngine[T, V]) 
    (implicit clients: RedisClientPool, 
              parse: Parse[V], 
              m: Monoid[V], 
              ex: Equal[V], 
              p: Parse[MatchList[V]], 
              f: Format) = loadBalanced(4, actorOf(new FileTransformer[T, V](engine)))

  def loadingService[T, V](engine: ReconEngine[T, V]) 
    (implicit clients: RedisClientPool, 
              format: Format, 
              parse: Parse[V], 
              m: Monoid[V], 
              p: ReconProtocol[T, V], 
              mv: Manifest[V]) = loadBalanced(4, actorOf(new Loader[T, V](engine)))

  case class Transform[T](fileName: String, config: ReconSource[T])

  class FileTransformer[T, V](engine: ReconEngine[T, V]) 
    (implicit clients: RedisClientPool, 
              parse: Parse[V], 
              m: Monoid[V], 
              ex: Equal[V], 
              p: Parse[MatchList[V]], 
              f: Format) extends Actor {
    self.dispatcher = reconDispatcher

    def receive = {
      case Transform(f, c) => self.reply(engine.fromSource1((f, c.asInstanceOf[ReconSource[T]])))
    }
  }

  class Aggregator[T, V](engine: ReconEngine[T, V], completionPred: List[String] => Boolean) 
    (implicit clients: RedisClientPool, 
              parse: Parse[V], 
              m: Monoid[V], 
              ex: Equal[V], 
              mv: Manifest[V], 
              r: ReconProtocol[T, V], 
              p: Parse[MatchList[V]], 
              f: Format) extends Actor {
    self.dispatcher = reconDispatcher
    val ids = collection.mutable.ListBuffer.empty[String]
    def receive = {
      case Some(rdef) => loadingService[T, V](engine).tell(rdef, self)
      case id: String => 
        ids append id
        println("received in Aggregator: " + id + " size = " + ids.toList.size + " pred = " + completionPred(ids.toList))
        if (completionPred(ids.toList)) {
          println("completion done ..")
          val processor = actorOf(new Processor(engine)).start()
          // processor.tell(ids.toList.sorted)
          processor.tell(ids.toList.sortWith(_ > _))
        }
    }
  }

  abstract class ReconConsumer[T, V](engine: ReconEngine[T, V], completionPred: List[String] => Boolean)
    (implicit clients: RedisClientPool, 
              parse: Parse[V], 
              m: Monoid[V], 
              v: Manifest[V], 
              r: ReconProtocol[T, V], 
              ex: Equal[V], 
              p: Parse[MatchList[V]], 
              f: Format) extends Actor with Consumer {
  
    def endpointUri: String
    self.dispatcher = reconDispatcher
    val aggregator = actorOf(new Aggregator(engine, completionPred)).start()
  
    def getSourceConfig(file: String): ReconSource[T]
  
    def receive = {
      case msg: Message => 
        val fileName = msg.getHeaderAs("CamelFilePath", classOf[String])
        fileTransformationService(engine).tell(Transform(fileName, getSourceConfig(fileName)), aggregator)
    }
  }
  
  class Loader[T, V](engine: ReconEngine[T, V])
    (implicit clients: RedisClientPool, 
              format: Format, 
              parse: Parse[V], 
              m: Monoid[V], 
              p: ReconProtocol[T, V], 
              mv: Manifest[V]) extends Actor {
  
    self.dispatcher = reconDispatcher
    def receive = {
      case rdef: ReconDef[_] => 
        println("in Loader .. processing " + rdef.values.size)
        self.reply(engine.loadOneReconSet(rdef.asInstanceOf[ReconDef[T]]))
      case _ => sys.error("error")
    }
  }
  
  class Processor[T, V](engine: ReconEngine[T, V])
    (implicit clients: RedisClientPool, 
              parse: Parse[V], 
              m: Monoid[V], 
              ex: Equal[V], 
              p: Parse[MatchList[V]], 
              f: Format) extends Actor {
  
    self.dispatcher = reconDispatcher
    def receive = {
      case ids: List[String] =>
        println("in propcessor .. " + ids)
        val res = engine.reconcile(ids, matchHeadAsSumOfRest).seq
        println("after reconcile ..")
        println(engine.persist(res))
        println(System.currentTimeMillis)
      case _ => sys.error("error")
    }
  }
}
