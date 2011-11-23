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
import BalanceReconEngine._
import MatchFunctions._
import Util._

import scalaz._
import Scalaz._

@RunWith(classOf[JUnitRunner])
class ReconSpec extends Spec 
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

  describe("run recon with a 1:1 balance matching data set") {
    it("should generate report") {
      val bs1 = 
        List(
          Balance("a-123", now, "USD", 1000), 
          Balance("a-134", now, "USD", 2000))

      val bs2 = 
        List(
          Balance("a-123", now, "USD", 1000), 
          Balance("a-134", now, "USD", 2000))

      val defs = Seq(CollectionDef("r21", bs1), CollectionDef("r22", bs2))

      val res1 = 
        loadInput[Balance, Int](defs) 
          .fold(_ => none, reconcile[Int](_, match1on1).seq.some) map 
            persist[Int]

      res1.foreach {m =>
        (m get Match) should equal(Some(Some(2)))
        (m get Break) should equal(Some(Some(0)))
        (m get Unmatch) should equal(Some(Some(0)))
      }
    }
  }

  describe("run recon with another 1:1 balance matching data set") {
    it("should generate report") {
      val bs1 = 
        List(
          Balance("a-123", now, "USD", 1000), 
          Balance("a-134", now, "AUD", 2000),
          Balance("a-134", now, "GBP", 2500),
          Balance("a-136", now, "GBP", 2500),
          Balance("a-123", now, "JPY", 250000))

      val bs2 = 
        List(
          Balance("a-123", now, "USD", 126000), 
          Balance("a-124", now, "USD", 26000), 
          Balance("a-134", now, "AUD", 3250))

      val defs = Seq(CollectionDef("r21", bs1), CollectionDef("r22", bs2))
      val res1 = 
        loadInput[Balance, Int](defs) 
          .fold(_ => none, reconcile[Int](_, match1on1).seq.some) map 
            persist[Int]

      res1.foreach {m =>
        (m get Match) should equal(Some(Some(1)))
        (m get Break) should equal(Some(Some(2)))
        (m get Unmatch) should equal(Some(Some(1)))
      }
    }
  }

  describe("run recon with a unbalanced matching data set") {
    it("should generate report") {
      val now = DateTime.now.toLocalDate
      val bs1 = 
        List(
          Balance("a-1", now, "USD", 1000), 
          Balance("a-2", now, "USD", 2000),
          Balance("a-3", now, "USD", 2500),
          Balance("a-4", now, "USD", 2500))

      val bs2 = 
        List(
          Balance("a-1", now, "USD", 300), 
          Balance("a-2", now, "USD", 1000), 
          Balance("a-4", now, "USD", 500), 
          Balance("a-3", now, "USD", 2000))

      val bs3 = 
        List(
          Balance("a-1", now, "USD", 700), 
          Balance("a-2", now, "USD", 1000), 
          Balance("a-4", now, "USD", 2000), 
          Balance("a-3", now, "USD", 500))

      val defs = Seq(CollectionDef("r31", bs1), CollectionDef("r32", bs2), CollectionDef("r33", bs3))
      val res1 = 
        loadInput[Balance, Int](defs) 
          .fold(_ => none, reconcile[Int](_, matchHeadAsSumOfRest).seq.some) map 
            persist[Int]

      res1.foreach {m =>
        (m get Match) should equal(Some(Some(4)))
        (m get Break) should equal(Some(Some(0)))
        (m get Unmatch) should equal(Some(Some(0)))
      }
    }
  }

  describe("run recon with a 1:1 balance matching data set and predicate") {
    it("should generate report") {
      val now = DateTime.now.toLocalDate
      val bs1 = 
        List(
          Balance("a-123", now, "USD", 1000), 
          Balance("a-134", now, "AUD", 2000),
          Balance("a-134", now, "GBP", 2500),
          Balance("a-136", now, "GBP", 2500),
          Balance("a-123", now, "USD", 50),
          Balance("a-123", now, "JPY", 250000))

      val bs2 = 
        List(
          Balance("a-123", now, "USD", 126000), 
          Balance("a-124", now, "USD", 26000), 
          Balance("a-134", now, "USD", 50), 
          Balance("a-134", now, "AUD", 3250))

      val gr100 = (b: Balance) => b.amount > 100

      val defs = Seq(CollectionDef("r21", bs1, Some(gr100)), CollectionDef("r22", bs2, Some(gr100)))
      val res1 = 
        loadInput[Balance, Int](defs) 
          .fold(_ => none, reconcile[Int](_, match1on1).seq.some) map 
            persist[Int]

      res1.foreach {m =>
        (m get Match) should equal(Some(Some(1)))
        (m get Break) should equal(Some(Some(2)))
        (m get Unmatch) should equal(Some(Some(1)))
      }
    }
  }

  describe("generate data") {
    it("should generate data") {
      import ReconDataGenerator._
      val (m, s1, s2) = generateDataForMultipleAccounts
      val start = System.currentTimeMillis

      val defs = Seq(CollectionDef("r41", m), CollectionDef("r42", s1), CollectionDef("r43", s2))
      val r1 = loadInput[Balance, Int](defs)
      val afterLoad = System.currentTimeMillis
      println("load time = " + (afterLoad - start))

      val res1 = r1.fold(_ => none, reconcile[Int](_, matchHeadAsSumOfRest).seq.some) map persist[Int]

      val end = System.currentTimeMillis
      println("recon time = " + (end - afterLoad))
      res1.foreach {m =>
        (m get Match) should equal(Some(Some(1000)))
        (m get Break) should equal(Some(Some(0)))
        (m get Unmatch) should equal(Some(Some(0)))
      }
    }
  }
}

