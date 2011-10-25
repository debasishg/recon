package recon

import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scala_tools.time.Imports._

import com.redis._
import BalanceRecon._

@RunWith(classOf[JUnitRunner])
class ReconSpec extends Spec 
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

  describe("load data into redis") {
    it("should load into hash") {
      val now = DateTime.now.toLocalDate
      val balances = 
        List(
          Balance("a-123", now, "USD", 1000), 
          Balance("a-134", now, "AUD", 2000),
          Balance("a-134", now, "GBP", 2500),
          Balance("a-123", now, "JPY", 250000))
      loadBalance("r1", balances)
      println(getBalance("r11"))
    }
  }

  describe("run recon with a 1:1 balance matching data set") {
    it("should generate report") {
      val now = DateTime.now.toLocalDate
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

      println(loadBalances(Map("r21" -> bs1, "r22" -> bs2)))

      def matchFn(maybeVals: List[Option[Int]]) = {
        maybeVals.flatten.size match {
          case l if l == maybeVals.size => maybeVals.head == maybeVals.tail.head
          case _ => false
        }
      }
      try {
        reconBalance(List("r21", "r22"), matchFn).foreach(println)
      } catch { case ex: Exception => ex.printStackTrace }
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

      println(loadBalances(Map("r31" -> bs1, "r32" -> bs2, "r33" -> bs3)))

      def matchFn(maybeVals: List[Option[Int]]) = {
        val flist = maybeVals.flatten
        flist.size match {
          case l if l == maybeVals.size => flist match {
            case (a :: b :: c :: Nil) => a == (b + c)
            case _ => false
          }
          case _ => false
        }
      }
      reconBalance(List("r31", "r32", "r33"), matchFn).foreach(println)
    }
  }
}

