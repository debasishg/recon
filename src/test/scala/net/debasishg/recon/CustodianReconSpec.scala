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
          loadInput[CustodianFetchValue, Double]) map 
            (_.fold(_ => none, reconcile[Double](_, matchHeadAsSumOfRest).seq.some))) map2 
              persist[Double]

      val m1 = Map() ++ res.flatten.flatten 
      (m1 get Match) should equal(Some(Some(66)))
      (m1 get Break) should equal(Some(Some(52)))
      (m1 get Unmatch) should equal(Some(Some(8)))
    }
  }

  describe("Custodian A B and C") {
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
          loadInput[CustodianFetchValue, Double]) map 
            (_.fold(_ => none, reconcile[Double](_, matchHeadAsSumOfRest).seq.some))) map2 
              persist[Double]

      val m1 = Map() ++ res1.flatten.flatten 
      (m1 get Match) should equal(Some(Some(66)))
      (m1 get Break) should equal(Some(Some(52)))
      (m1 get Unmatch) should equal(Some(Some(8)))

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
          engine2.loadInput[CustodianFetchValue, Double]) map 
            (_.fold(_ => none, engine2.reconcile[Double](_, matchHeadAsSumOfRest).seq.some))) map2 
              engine2.persist[Double]

      val m2 = Map() ++ res2.flatten.flatten 
      (m2 get Match) should equal(Some(Some(69)))
      (m2 get Break) should equal(Some(Some(52)))
      (m2 get Unmatch) should equal(Some(Some(5)))

      engine2.consolidateWith[Double](engine.runDate)

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
          engine3.loadInput[CustodianFetchValue, Double]) map 
            (_.fold(_ => none, engine3.reconcile[Double](_, matchHeadAsSumOfRest).seq.some))) map2 
              engine3.persist[Double]

      val m3 = Map() ++ res3.flatten.flatten 
      (m3 get Match) should equal(Some(Some(72)))
      (m3 get Break) should equal(Some(Some(52)))
      (m3 get Unmatch) should equal(Some(Some(2)))

      engine3.consolidateWith[Double](engine2.runDate)
    }
  }
}

