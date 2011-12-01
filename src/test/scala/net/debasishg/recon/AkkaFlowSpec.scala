package net.debasishg.recon

import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import scala.util.continuations._
import akka.actor.{Actor, ActorRef}
import akka.actor.Actor._
import akka.dispatch.{Promise, Future, CompletableFuture}
import Future._

case class MessageX(text: String)
class Recipient(id: Int) extends Actor {
  
  def receive = {
    case MessageX(msg) => 
      Thread.sleep(1000)
      self.reply("%s, [%s]! ".format(msg, id))
  }
}

class Aggregator(recipients: Iterable[ActorRef]) extends Actor{
    
  def receive = {
    case msg @ MessageX(text) => 
      println("Started processing message `%s`" format(text))
      
      val result = Promise[String]()          
      val promises = List.fill(recipients.size)(Promise[String]())
      
      recipients.zip(promises).map{case (recipient, promise) =>
        (recipient ? msg).mapTo[String].map {result: String => 
          println("Binding recipient's response: %s" format(result))
          flow{        
            promise << result
          } 
        }
      }
      
      flow{        
        def gather(promises: List[CompletableFuture[String]], result: String = ""): String @cps[Future[Any]] = 
          promises match {
            case head :: tail => gather(tail, head() + result)
            case Nil => result  
          }
            
        println("Binding result...")
        result << gather(promises)
      }
      
      self.reply(result)
  }
}

@RunWith(classOf[JUnitRunner])
class AkkaFlowSpec extends Spec 
                   with ShouldMatchers
                   with BeforeAndAfterEach
                   with BeforeAndAfterAll {

  describe("dataflow") {
    it("should work") {
      val recipients = (1 to 5).map(i => actorOf(new Recipient(i)).start)
      val aggregator = actorOf(new Aggregator(recipients)).start
      val results1 = aggregator ? MessageX("Hello")
      val results2 = aggregator ? MessageX("world")
      results1.map{ res => 
        println("Result: %s" format(res.asInstanceOf[Future[String]].get))
      }
      results2.map{ res => 
        println("Result: %s" format(res.asInstanceOf[Future[String]].get))
      }  
    }
  }
}
