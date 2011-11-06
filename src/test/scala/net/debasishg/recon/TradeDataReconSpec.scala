package net.debasishg.recon

import scala.collection.parallel.ParSet
import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scala_tools.time.Imports._

import com.redis._
import TradeDataRecon._
import MatchFunctions._

@RunWith(classOf[JUnitRunner])
class TradeDataReconSpec extends Spec 
                         with ShouldMatchers
                         with BeforeAndAfterEach
                         with BeforeAndAfterAll {

  implicit val clients = new RedisClientPool("localhost", 6379)

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

      val l = loadAllTradeData(Seq(CollectionDef("r21", bs1), CollectionDef("r22", bs2)))
      l.size should equal(2)
      reconTradeData(List("r21", "r22"), match1on1).seq.foreach(println)
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

      val l = loadAllTradeData(Seq(CollectionDef("r21", bs1), CollectionDef("r22", bs2)))
      l.size should equal(2)
      reconTradeData(List("r21", "r22"), match1on1).seq.foreach(println)
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

      val l = loadAllTradeData(Seq(CollectionDef("r21", bs1), CollectionDef("r22", bs2)))
      l.size should equal(2)
      reconTradeData(List("r21", "r22"), match1on1).seq.foreach(println)
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

      val l = loadAllTradeData(Seq(CollectionDef("r21", bs1), CollectionDef("r22", bs2), CollectionDef("r23", bs3)))
      l.size should equal(3)

      reconTradeData(List("r23", "r21", "r22"), matchHeadAsSumOfRest).seq.foreach(println)
    }
  }
}
