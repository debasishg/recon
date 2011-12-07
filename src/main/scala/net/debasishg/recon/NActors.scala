package net.debasishg.recon

import collection.mutable.ListBuffer
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
import akka.dispatch.{Future, ActorCompletableFuture, DefaultCompletableFuture}
import akka.scalaz.futures._

object ReconNActors {

  private[this] final val reconDispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("recon-dispatcher")
    .withNewThreadPoolWithLinkedBlockingQueueWithCapacity(100)
    .setCorePoolSize(20)
    .build

  private[this] def loadBalanced(poolSize: Int, actor: ⇒ ActorRef): ActorRef = {
    val workers = Vector.fill(poolSize)(actor.start())
    Routing.loadBalancerActor(CyclicIterator(workers)).start()
  }

  // sets up the pool and starts the worker actors
  private[this] def fileTransformationService[T, V](engine: ReconEngine[T, V]) 
    (implicit clients: RedisClientPool, 
              parse: Parse[V], 
              m: Monoid[V], 
              ex: Equal[V], 
              p: Parse[MatchList[V]], 
              f: Format) = loadBalanced(4, actorOf(new FileTransformerWorker[T, V](engine)))

  private[this] def loadingService[T, V](engine: ReconEngine[T, V]) 
    (implicit clients: RedisClientPool, 
              format: Format, 
              parse: Parse[V], 
              m: Monoid[V], 
              p: ReconProtocol[T, V], 
              mv: Manifest[V]) = loadBalanced(4, actorOf(new LoaderWorker[T, V](engine)))

  private[this] case class Transform[T](fileName: String, config: ReconSource[T])
  private[this] case class FileTransformed[T](fs: List[Option[ReconDef[T]]])
  private[this] case class StoredInRepository(ids: List[String])
  private[this] case class Reconciled[V: Monoid](rs: Set[ReconResult[V]])
  private[this] case class Persisted(rs: Int)

  private[this] class FileTransformerWorker[T, V](engine: ReconEngine[T, V]) 
    (implicit clients: RedisClientPool, 
              parse: Parse[V], 
              m: Monoid[V], 
              ex: Equal[V], 
              p: Parse[MatchList[V]], 
              f: Format) extends Actor {
    self.dispatcher = reconDispatcher

    override def receive = {
      case Transform(f, c) => 
        self.reply(engine.fromSource1((f, c.asInstanceOf[ReconSource[T]])))
    }
  }

  private[this] class FileTransformerAggregator[T, V](engine: ReconEngine[T, V], 
    completionPred: List[Future[_]] => Boolean) 
    (implicit clients: RedisClientPool, 
              parse: Parse[V], 
              m: Monoid[V], 
              v: Manifest[V], 
              r: ReconProtocol[T, V], 
              ex: Equal[V], 
              p: Parse[MatchList[V]], 
              f: Format) extends Actor {

    self.dispatcher = reconDispatcher
    val fs = ListBuffer.empty[Future[Any]]
    val pool = fileTransformationService(engine)

    override def receive = {
      case msg => 
        fs append (pool ? msg)
        if (completionPred(fs.toList)) {
          self.reply(FileTransformed(Future.sequence(fs.toList).get.asInstanceOf[List[Option[ReconDef[T]]]]))
        }
    }

    override def postStop() = {
      pool.stop()
    }
  }

  abstract class ReconConsumer[T, V](engine: ReconEngine[T, V], completionPred: List[Future[_]] => Boolean)
    (implicit clients: RedisClientPool, 
              p: Parse[MatchList[V]], 
              parse: Parse[V], 
              m: Monoid[V], 
              v: Manifest[V], 
              r: ReconProtocol[T, V], 
              ex: Equal[V], 
              f: Format) extends Actor with Consumer {
  
    def endpointUri: String
    self.dispatcher = reconDispatcher

    val fileXformerAggregator = actorOf(new FileTransformerAggregator(engine, completionPred)).start()
    val loaderAggregator = actorOf(new LoaderAggregator(engine, completionPred)).start()
    val processor = actorOf(new Processor(engine)).start()
    val persister = actorOf(new Persister(engine)).start()
  
    def getSourceConfig(file: String): ReconSource[T]
  
    override def receive = {
      case msg: Message => 
        val fileName = msg.getHeaderAs("CamelFilePath", classOf[String])
        fileXformerAggregator ! Transform(fileName, getSourceConfig(fileName))

      case FileTransformed(rs) => 
        val rdefs = rs.asInstanceOf[List[Option[ReconDef[T]]]]
        if (rdefs.size != rdefs.flatten.size) 
          println("error in file transformation")
        else 
          rdefs.flatten.map {r: ReconDef[T] => loaderAggregator ! r}

      case StoredInRepository(ids) => processor ! ids 

      case Reconciled(reconResults) => persister ! reconResults 

      case Persisted(s) => 
        clients.withClient {client => 
          println(ReconUtils.fetchBreakEntries(client, engine.clientName, engine.runDate))
        }
        fileXformerAggregator.stop()
        loaderAggregator.stop()
        processor.stop()
        persister.stop()
        println(System.currentTimeMillis)
    }
  }
  
  private[this] class LoaderAggregator[T, V](engine: ReconEngine[T, V], 
    completionPred: List[Future[_]] => Boolean)
    (implicit clients: RedisClientPool, 
              format: Format, 
              parse: Parse[V], 
              m: Monoid[V], 
              p: ReconProtocol[T, V], 
              mv: Manifest[V]) extends Actor {
  
    self.dispatcher = reconDispatcher
    val rdefs = ListBuffer.empty[Future[Any]]
    val pool = loadingService[T, V](engine)

    override def receive = {
      case rdef: ReconDef[_] => 
        rdefs append (pool ? rdef)
        if (completionPred(rdefs.toList)) 
          self.reply(StoredInRepository(Future.sequence(rdefs.toList).get.asInstanceOf[List[String]]))
    }

    override def postStop() = {
      pool.stop()
    }
  }

  private[this] class LoaderWorker[T, V](engine: ReconEngine[T, V])
    (implicit clients: RedisClientPool, 
              format: Format, 
              parse: Parse[V], 
              m: Monoid[V], 
              p: ReconProtocol[T, V], 
              mv: Manifest[V]) extends Actor {
  
    self.dispatcher = reconDispatcher
    override def receive = {
      case rdef: ReconDef[_] => 
        self.reply(engine.loadOneReconSet(rdef.asInstanceOf[ReconDef[T]]))
    }
  }

  private[this] class Processor[T, V](engine: ReconEngine[T, V])
    (implicit clients: RedisClientPool, 
              parse: Parse[V], 
              m: Monoid[V], 
              ex: Equal[V]) extends Actor {
  
    self.dispatcher = reconDispatcher
    override def receive = {
      case ids: List[String] =>
        self.reply(Reconciled(engine.reconcile(ids, matchHeadAsSumOfRest).seq))
    }
  }

  private[this] class Persister[T, V](engine: ReconEngine[T, V])
    (implicit clients: RedisClientPool, 
              m: Monoid[V], 
              p: Parse[MatchList[V]], 
              f: Format) extends Actor {
  
    self.dispatcher = reconDispatcher
    override def receive = {
      case res: Set[_] =>
        self.reply(Persisted(engine.persist(res.asInstanceOf[Set[ReconResult[V]]])))
    }
  }
}
