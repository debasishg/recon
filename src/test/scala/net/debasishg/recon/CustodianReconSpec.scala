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
import com.redis.serialization._
import org.joda.time.{DateTime, LocalDate}

import scalaz._
import Scalaz._

@RunWith(classOf[JUnitRunner])
class CustodianReconSpec extends Spec 
                         with ShouldMatchers
                         with BeforeAndAfterEach
                         with BeforeAndAfterAll {

  implicit val clients = new RedisClientPool("localhost", 6379)
  implicit val format = Format {case l: MatchList[Double] => serializeMatchList(l)}
  implicit val parseList = Parse[MatchList[Double]](deSerializeMatchList[Double](_))

  val path = "/home/debasish/my-projects/reconciliation/recon/src/test/resources/australia/"

  override def beforeEach = {
  }

  override def afterEach = clients.withClient{
    client => client.flushdb
  }

  override def afterAll = {
    clients.withClient {client => client.disconnect}
    clients.close
  }

  describe("Custodian A B and C for 2010-10-24") {
    it("should load csv data from file") {
      val engine = new CustodianReconEngine {
        override val runDate = new DateTime("2010-10-24").toLocalDate
      }
      import engine._

      // load from files
      val files1 = List(
        (path + "20101024/DATA_CUSTODIAN_C.csv", CustodianCConfig),
        (path + "20101024/DATA_CUSTODIAN_A.csv", CustodianAConfig),
        (path + "20101024/DATA_CUSTODIAN_B.txt", CustodianBConfig))

      import Parse.Implicits.parseDouble

      val res = 
        ((fromSource(files1) map 
          loadInput) map 
            (_.fold(_ => none, reconcile(_, matchHeadAsSumOfRest).seq.some))) map2 
              persist

      import ReconUtils._
      clients.withClient {client =>
        fetchMatchEntries[Double](client, clientName, runDate).map(_.size) should equal(Some(66))
        fetchUnmatchEntries[Double](client, clientName, runDate).map(_.size) should equal(Some(8))
        fetchBreakEntries[Double](client, clientName, runDate).map(_.size) should equal(Some(52))
      }
    }
  }

  describe("Custodian A B and C for all 3 run days") {
    it("should load csv data from file") {
      val engine = new CustodianReconEngine {
        override val runDate = new DateTime("2010-10-24").toLocalDate
      }
      import engine._

      // load from files
      val files1 = List(
        (path + "20101024/DATA_CUSTODIAN_C.csv", CustodianCConfig),
        (path + "20101024/DATA_CUSTODIAN_A.csv", CustodianAConfig),
        (path + "20101024/DATA_CUSTODIAN_B.txt", CustodianBConfig))

      import Parse.Implicits.parseDouble

      type EitherEx[A] = Either[Throwable, A]

      val res1 = 
        ((fromSource(files1) map 
          loadInput) map 
            (_.fold(_ => none, reconcile(_, matchHeadAsSumOfRest).seq.some))) map2 
              persist

      import ReconUtils._
      clients.withClient {client =>
        fetchMatchEntries[Double](client, clientName, runDate)
          .map(_.size) should equal(Some(66))
        fetchUnmatchEntries[Double](client, clientName, runDate)
          .map(_.size) should equal(Some(8))
        fetchBreakEntries[Double](client, clientName, runDate)
          .map(_.size) should equal(Some(52))
      }

      val engine2 = new CustodianReconEngine {
        override val runDate = new DateTime("2010-10-25").toLocalDate
      }

      // load from files
      val files2 = List(
        (path + "20101025/DATA_CUSTODIAN_C.csv", CustodianCConfig),
        (path + "20101025/DATA_CUSTODIAN_A.csv", CustodianAConfig),
        (path + "20101025/DATA_CUSTODIAN_B.txt", CustodianBConfig))

      val res2 = 
        ((engine2.fromSource(files2) map 
          engine2.loadInput) map 
            (_.fold(_ => none, engine2.reconcile(_, matchHeadAsSumOfRest).seq.some))) map2 
              engine2.persist

      import ReconUtils._
      clients.withClient {client =>
        fetchMatchEntries[Double](client, engine2.clientName, engine2.runDate)
          .map(_.size) should equal(Some(69))
        fetchUnmatchEntries[Double](client, engine2.clientName, engine2.runDate)
          .map(_.size) should equal(Some(5))
        fetchBreakEntries[Double](client, engine2.clientName, engine2.runDate)
          .map(_.size) should equal(Some(52))
      }

      engine2.consolidateWith(engine.runDate)

      val engine3 = new CustodianReconEngine {
        override val runDate = new DateTime("2010-10-26").toLocalDate
      }

      // load from files
      val files3 = List(
        (path + "20101026/DATA_CUSTODIAN_C.csv", CustodianCConfig),
        (path + "20101026/DATA_CUSTODIAN_A.csv", CustodianAConfig),
        (path + "20101026/DATA_CUSTODIAN_B.txt", CustodianBConfig))

      val res3 = 
        ((engine3.fromSource(files3) map 
          engine3.loadInput) map 
            (_.fold(_ => none, engine3.reconcile(_, matchHeadAsSumOfRest).seq.some))) map2 
              engine3.persist

      import ReconUtils._
      clients.withClient {client =>
        fetchMatchEntries[Double](client, engine3.clientName, engine3.runDate)
          .map(_.size) should equal(Some(72))
        fetchUnmatchEntries[Double](client, engine3.clientName, engine3.runDate)
          .map(_.size) should equal(Some(2))
        fetchBreakEntries[Double](client, engine3.clientName, engine3.runDate)
          .map(_.size) should equal(Some(52))
      }

      engine3.consolidateWith(engine2.runDate)
    }
  }

  describe("Custodian A B and C for query") {
    it("should load csv data from file") {
      val engine = new CustodianReconEngine {
        override val runDate = new DateTime("2010-10-24").toLocalDate
      }
      import engine._

      // load from files
      val files1 = List(
        (path + "20101024/DATA_CUSTODIAN_C.csv", CustodianCConfig),
        (path + "20101024/DATA_CUSTODIAN_A.csv", CustodianAConfig),
        (path + "20101024/DATA_CUSTODIAN_B.txt", CustodianBConfig))

      import Parse.Implicits.parseDouble

      val res = 
        ((fromSource(files1) map 
          loadInput) map 
            (_.fold(_ => none, reconcile(_, matchHeadAsSumOfRest).seq.some))) map2 
              persist

      import ReconUtils._
      clients.withClient {client =>
        fetchMatchEntries[Double](client, clientName, runDate)
          .map(_.size) should equal(Some(66))
        fetchUnmatchEntries[Double](client, clientName, runDate)
          .map(_.size) should equal(Some(8))
        fetchBreakEntries[Double](client, clientName, runDate)
          .map(_.size) should equal(Some(52))
      }
    }
  }
}
