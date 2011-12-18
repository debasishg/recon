package net.debasishg.recon

import scala.collection.parallel.ParSet
import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import com.redis._
import MatchFunctions._

import sjson.json.DefaultProtocol._
import Util._
import FileUtils._
import com.redis.serialization._
import org.joda.time.{DateTime, LocalDate}

import scalaz._
import Scalaz._

import akka.actor.{Actor, ActorRef}
import akka.camel.{Message, Consumer}
import akka.camel.CamelServiceManager._
import akka.actor.Actor._
import akka.camel.CamelContextManager
import ReconActors._

class CReconConsumer(engine: ReconEngine[Balance, Int], totalNoOfFiles: Int)
  (implicit clients: RedisClientPool, 
            parse: Parse[Int], 
            m: Monoid[Int], 
            r: ReconProtocol[Balance, Int], 
            p: Parse[MatchList[Int]], 
            f: Format) extends ReconConsumer[Balance, Int](engine, totalNoOfFiles) {

  override def endpointUri = "file:/Users/debasishghosh/balance?noop=true&include=.*\\.csv&sortBy=file:name"

  override def getSourceConfig(file: String): ReconSource[Balance] =
    if (file contains "main") BalanceMainConfig
    else if (file contains "sub1") BalanceSub1Config
    else BalanceSub2Config
}

@RunWith(classOf[JUnitRunner])
class BalanceReconCamelSpec extends Spec 
                              with ShouldMatchers
                              with BeforeAndAfterEach
                              with BeforeAndAfterAll {

  implicit val clients = new RedisClientPool("localhost", 6379)
  implicit val format = Format {case l: MatchList[Int] => serializeMatchList(l)}
  implicit val parseList = Parse[MatchList[Int]](deSerializeMatchList[Int](_))
  import Parse.Implicits.parseInt

  override def beforeEach = {
  }

  // override def afterEach = clients.withClient{ client => client.flushdb }

  override def afterAll = {
    // clients.withClient {client => client.disconnect}
    // clients.close
  }

  def runReconFor(date: LocalDate, dateString: String) = {
    val engine = new BalanceReconEngine {
      override val runDate = date
    }
    import engine._

    // val proc = actorOf(
      // new ReconProcessor[Balance, Int](engine, (x: List[String]) => x.size == 3)).start
    // val loader = actorOf(new ReconLoader[Balance, Int](engine, proc)).start
    actorOf(new CReconConsumer(engine, 3)).start // create Consumer actor
  }

  describe("Custodian A B and C for 2010-10-24") {
    it("should load csv data from file") {
      startCamelService
      CamelContextManager.init  // optionally takes a CamelContext as argument
      CamelContextManager.start // starts the managed CamelContext

      val start = System.currentTimeMillis
      println("started at : " + start)
      runReconFor(new DateTime("2010-10-24").toLocalDate, "20101024")
      println("elapsed = " + (System.currentTimeMillis - start))
    }
  }
}
