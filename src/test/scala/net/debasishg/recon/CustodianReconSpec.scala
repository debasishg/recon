package net.debasishg.recon

import scala.collection.parallel.ParSet
import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import com.redis._
import CustodianRecon._
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

  describe("Custodian A B and C") {
    it("should load csv data from file") {
      val r1 = CustodianConfig.run(
        List(
          (path + "20101024/DATA_CUSTODIAN_C.csv", "C"),
          (path + "20101024/DATA_CUSTODIAN_A.csv", "A"),
          (path + "20101024/DATA_CUSTODIAN_B.txt", "B")), "20101024")

      type EitherEx[A] = Either[Throwable, A]

      val rd1 = new DateTime("2010-10-24").toLocalDate

      val rdefs = r1.seq.map {case (id, coll) => 
        CollectionDef(id, coll.flatten.flatten)
      }

      val res1 = 
      loadCustodianFetchValues(rdefs, rd1)
        .sequence[EitherEx, String]
        .fold(_ => none, reconCustodianFetchValue(_, matchHeadAsSumOfRest, rd1).seq.some)

      println(res1)

      // res1.foreach(println)

      /**
      res1.filter(_.result == Unmatch).foreach(println)
      res1.filter(_.result == Match).size should equal(66)
      res1.filter(_.result == Unmatch).size should equal(8)
      res1.filter(_.result == Break).size should equal(52)

      val m1 = engine.persist(res1, rd1)
      (m1 get Match) should equal(Some(Some(66)))
      (m1 get Break) should equal(Some(Some(52)))
      (m1 get Unmatch) should equal(Some(Some(8)))

      val rd2 = new DateTime("2010-10-25").toLocalDate
      val r2 = CustodianConfig.run(
        List(
          (path + "20101025/DATA_CUSTODIAN_C.csv", "C"),
          (path + "20101025/DATA_CUSTODIAN_A.csv", "A"),
          (path + "20101025/DATA_CUSTODIAN_B.txt", "B")), "20101025")

      val res2 = 
      loadCustodianFetchValues(
        r2.seq.map {case (id, coll) => CollectionDef(id, coll.flatten.flatten)}, rd2)
             .sequence[EitherEx, String]
             .fold(_ => sys.error("unexpected"), reconCustodianFetchValue(_, matchHeadAsSumOfRest, rd2).seq)

      println("---------------------------")
      res2.filter(_.result == Unmatch).foreach(println)
      res2.filter(_.result == Match).size should equal(69)
      res2.filter(_.result == Unmatch).size should equal(5)
      res2.filter(_.result == Break).size should equal(52)

      val m2 = engine.persist(res2, rd2)
      (m2 get Match) should equal(Some(Some(69)))
      (m2 get Break) should equal(Some(Some(52)))
      (m2 get Unmatch) should equal(Some(Some(5)))

      engine.consolidateWith[Double](rd1)

      val rd3 = new DateTime("2010-10-26").toLocalDate
      val r3 = CustodianConfig.run(
        List(
          (path + "20101026/DATA_CUSTODIAN_C.csv", "C"),
          (path + "20101026/DATA_CUSTODIAN_A.csv", "A"),
          (path + "20101026/DATA_CUSTODIAN_B.txt", "B")), "20101026")

      val res3 = 
      loadCustodianFetchValues(
        r3.seq.map {case (id, coll) => CollectionDef(id, coll.flatten.flatten)}, rd3)
             .sequence[EitherEx, String]
             .fold(_ => sys.error("unexpected"), reconCustodianFetchValue(_, matchHeadAsSumOfRest, rd3).seq)

      println("---------------------------")
      res3.filter(_.result == Unmatch).foreach(println)
      res3.filter(_.result == Match).size should equal(72)
      res3.filter(_.result == Unmatch).size should equal(2)
      res3.filter(_.result == Break).size should equal(52)

      val m3 = engine.persist(res3, rd3)
      (m3 get Match) should equal(Some(Some(72)))
      (m3 get Break) should equal(Some(Some(52)))
      (m3 get Unmatch) should equal(Some(Some(2)))

      engine.consolidateWith[Double](rd2)
    **/
    }
  }

  /**
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

      val m = engine persist res 
      (m get Match) should equal(Some(Some(66)))
      (m get Break) should equal(Some(Some(52)))
      (m get Unmatch) should equal(Some(Some(8)))
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
  **/
}

