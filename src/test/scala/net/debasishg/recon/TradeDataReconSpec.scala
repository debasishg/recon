package net.debasishg.recon

import scala.collection.parallel.ParSet
import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scala_tools.time.Imports._

import sjson.json.DefaultProtocol._
import com.redis._
import com.redis.serialization._
import Parse.Implicits.parseInt
import TradeDataReconEngine._
import MatchFunctions._
import Util._

import scalaz._
import Scalaz._

@RunWith(classOf[JUnitRunner])
class TradeDataReconSpec extends Spec 
                         with ShouldMatchers
                         with BeforeAndAfterEach
                         with BeforeAndAfterAll {

  implicit val clients = new RedisClientPool("localhost", 6379)
  implicit val format = Format {case l: MatchList[Int] => serializeMatchList(l)}
  implicit val parseList = Parse[MatchList[Int]](deSerializeMatchList[Int](_))
  type EitherEx[A] = Either[Throwable, A]

  override def beforeEach = {
  }

  override def afterEach = clients.withClient{
    client => client.flushdb
  }

  override def afterAll = {
    clients.withClient{ client => client.disconnect }
    clients.close
  }

  describe("run recon with a 1:1 trade matching data set 1") {
    it("should reconcile and generate report") {
      val now = DateTime.now.toLocalDate
      val bs1 = 
        List(
          TradeData("a-123", now, "GOOG", 100, 3500),
          TradeData("a-123", now, "IBM", 200, 4000),
          TradeData("a-134", now, "GOOG", 150, 4200))

      val bs2 = 
        List(
          TradeData("a-123", now, "GOOG", 100, 3500),
          TradeData("a-123", now, "IBM", 300, 6000),
          TradeData("a-134", now, "GOOG", 150, 4200))

      val defs = Seq(CollectionDef("r21", bs1), CollectionDef("r22", bs2))
      val res1 = 
        loadInput[TradeData, Int](defs)
          .sequence[EitherEx, String]
          .fold(_ => none, reconcile[Int](_, match1on1).seq.some) map persist[Int]

      res1.foreach {m =>
        (m get Match) should equal(Some(Some(2)))
        (m get Break) should equal(Some(Some(0)))
        (m get Unmatch) should equal(Some(Some(1)))
      }
    }
  }

  describe("1:1 trade matching data set with partial match") {
    it("should reconcile and generate report") {
      val now = DateTime.now.toLocalDate
      val bs1 = 
        List(
          TradeData("a-123", now, "GOOG", 100, 3500),
          TradeData("a-123", now, "IBM", 200, 4000),
          TradeData("a-134", now, "GOOG", 150, 4200)) // matches quantity but not security

      val bs2 = 
        List(
          TradeData("a-123", now, "GOOG", 100, 3500),
          TradeData("a-123", now, "IBM", 300, 6000),
          TradeData("a-134", now, "GOOG", 150, 4500))

      val defs = Seq(CollectionDef("r21", bs1), CollectionDef("r22", bs2))
      val res1 = 
        loadInput[TradeData, Int](defs)
          .sequence[EitherEx, String]
          .fold(_ => none, reconcile[Int](_, match1on1).seq.some) map persist[Int]

      res1.foreach {m =>
        (m get Match) should equal(Some(Some(1)))
        (m get Break) should equal(Some(Some(0)))
        (m get Unmatch) should equal(Some(Some(2)))
      }
    }
  }

  describe("1:1 trade matching data set unbalanced and with partial match") {
    it("should reconcile and generate report") {
      val now = DateTime.now.toLocalDate
      val bs1 = 
        List(
          TradeData("a-123", now, "GOOG", 100, 3500),
          TradeData("a-123", now, "IBM", 200, 4000),
          TradeData("a-156", now, "IBM", 200, 4000), // not present on other side
          TradeData("a-134", now, "GOOG", 150, 4200)) // matches quantity but not security

      val bs2 = 
        List(
          TradeData("a-123", now, "GOOG", 100, 3500),
          TradeData("a-123", now, "IBM", 300, 6000),
          TradeData("a-167", now, "IBM", 300, 6000),
          TradeData("a-134", now, "GOOG", 150, 4500))

      val defs = Seq(CollectionDef("r21", bs1), CollectionDef("r22", bs2))
      val res1 = 
        loadInput[TradeData, Int](defs)
          .sequence[EitherEx, String]
          .fold(_ => none, reconcile[Int](_, match1on1).seq.some) map persist[Int]

      res1.foreach {m =>
        (m get Match) should equal(Some(Some(1)))
        (m get Break) should equal(Some(Some(2)))
        (m get Unmatch) should equal(Some(Some(2)))
      }
    }
  }

  describe("a = b + c trade matching data set unbalanced and with partial match") {
    it("should reconcile and generate report") {
      val now = DateTime.now.toLocalDate
      val bs1 = 
        List(
          TradeData("a-123", now, "GOOG", 100, 3500),
          TradeData("a-123", now, "IBM", 200, 4000),
          TradeData("a-156", now, "IBM", 200, 4000), // not present on other side
          TradeData("a-134", now, "GOOG", 150, 4200)) // matches quantity but not security

      val bs2 = 
        List(
          TradeData("a-123", now, "GOOG", 100, 3500),
          TradeData("a-123", now, "IBM", 300, 6000),
          TradeData("a-167", now, "IBM", 300, 6000),
          TradeData("a-134", now, "GOOG", 150, 4500))

      val bs3 = 
        List(
          TradeData("a-123", now, "GOOG", 200, 7000),
          TradeData("a-123", now, "IBM", 500, 10000),
          TradeData("a-167", now, "IBM", 300, 6000),
          TradeData("a-134", now, "GOOG", 300, 8000))

      val defs = Seq(CollectionDef("r21", bs1), CollectionDef("r22", bs2), CollectionDef("r23", bs3))
      val res1 = 
        loadInput[TradeData, Int](defs)
          .sequence[EitherEx, String]
          .fold(_ => none, reconcile[Int](_, match1on1).seq.some) map persist[Int]

      res1.foreach {m =>
        (m get Match) should equal(Some(Some(0)))
        (m get Break) should equal(Some(Some(2)))
        (m get Unmatch) should equal(Some(Some(3)))
      }
    }
  }
}
