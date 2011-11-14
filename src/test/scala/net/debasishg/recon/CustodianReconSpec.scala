package net.debasishg.recon

import scala.collection.parallel.ParSet
import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import com.redis._
import CustodianRecon._
import MatchFunctions._

import scalaz._
import Scalaz._

@RunWith(classOf[JUnitRunner])
class CustodianReconSpec extends Spec 
                         with ShouldMatchers
                         with BeforeAndAfterEach
                         with BeforeAndAfterAll {

  implicit val clients = new RedisClientPool("localhost", 6379)
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

  describe("Custodian A B and C for 20101024") {
    it("should load csv data from file") {
      val r = CustodianConfig.run(
        List(
          (path + "20101024/DATA_CUSTODIAN_C.csv", "C"),
          (path + "20101024/DATA_CUSTODIAN_A.csv", "A"),
          (path + "20101024/DATA_CUSTODIAN_B.txt", "B")), "20101024")

      type EitherEx[A] = Either[Throwable, A]

      val res = 
      loadCustodianFetchValues(
        r.seq.map {case (id, coll) => CollectionDef(id, coll.flatten.flatten)})
             .sequence[EitherEx, String]
             .fold(_ => sys.error("unexpected"), reconCustodianFetchValue(_, matchHeadAsSumOfRest).seq)

      // res.foreach(println)
      res.filter(_.result == Match).size should equal(66)
      res.filter(_.result == Unmatch).size should equal(8)
      res.filter(_.result == Break).size should equal(52)
    }
  }

  describe("Custodian A B and C for 20101025") {
    it("should load csv data from file") {
      val r = CustodianConfig.run(
        List(
          (path + "20101025/DATA_CUSTODIAN_C.csv", "C"),
          (path + "20101025/DATA_CUSTODIAN_A.csv", "A"),
          (path + "20101025/DATA_CUSTODIAN_B.txt", "B")), "20101025")

      type EitherEx[A] = Either[Throwable, A]

      val res = 
      loadCustodianFetchValues(
        r.seq.map {case (id, coll) => CollectionDef(id, coll.flatten.flatten)})
             .sequence[EitherEx, String]
             .fold(_ => sys.error("unexpected"), reconCustodianFetchValue(_, matchHeadAsSumOfRest).seq)

      // res.foreach(println)
      res.filter(_.result == Match).size should equal(69)
      res.filter(_.result == Unmatch).size should equal(5)
      res.filter(_.result == Break).size should equal(52)
    }
  }

  describe("Custodian A B and C for 20101026") {
    it("should load csv data from file") {
      val r = CustodianConfig.run(
        List(
          (path + "20101026/DATA_CUSTODIAN_C.csv", "C"),
          (path + "20101026/DATA_CUSTODIAN_A.csv", "A"),
          (path + "20101026/DATA_CUSTODIAN_B.txt", "B")), "20101026")

      type EitherEx[A] = Either[Throwable, A]

      val res = 
      loadCustodianFetchValues(
        r.seq.map {case (id, coll) => CollectionDef(id, coll.flatten.flatten)})
             .sequence[EitherEx, String]
             .fold(_ => sys.error("unexpected"), reconCustodianFetchValue(_, matchHeadAsSumOfRest).seq)

      // res.foreach(println)
      res.filter(_.result == Match).size should equal(72)
      res.filter(_.result == Unmatch).size should equal(2)
      res.filter(_.result == Break).size should equal(52)
    }
  }

  describe("serialize") {
    it ("should serialize") {
      import sjson.json.DefaultProtocol._
      val l = List(None, Some(List(91.0, -2480.07)), None)
      import Util._
      println(deSerializeMatchList[Double](serializeMatchList(l)))
    }
  }
}

