package net.debasishg.recon

import scala.collection.parallel.ParSet
import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import com.redis._
import CustodianRecon._
import MatchFunctions._

@RunWith(classOf[JUnitRunner])
class CustodianReconSpec extends Spec 
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

  describe("Custodian A B and C") {
    it("should load csv data from file") {
      val a = CustodianAConfig.process("/Users/debasishghosh/projects/recon/src/test/resources/DATA_CUSTODIAN_A.csv")
      a.size should equal(39)
      val b = CustodianBConfig.process("/Users/debasishghosh/projects/recon/src/test/resources/b.txt")
      // b.size should equal(184)
      val c = CustodianCConfig.process("/Users/debasishghosh/projects/recon/src/test/resources/c.csv")
      // c.size should equal(499)
      val l = loadCustodianFetchValues(Seq(CollectionDef("ra", a.flatten.flatten), CollectionDef("rb", b.flatten.flatten), CollectionDef("rc", c.flatten.flatten)))
      l.size should equal(3)
      reconCustodianFetchValue(List("rc", "ra", "rb"), matchHeadAsSumOfRest).seq.foreach(println)
    }
  }
}

