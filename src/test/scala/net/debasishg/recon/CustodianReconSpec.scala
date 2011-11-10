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

  override def beforeEach = {
  }

  override def afterEach = clients.withClient{
    client => client.flushdb
  }

  override def afterAll = {
    clients.withClient{ client => client.disconnect }
    clients.close
  }

  /**
  describe("Custodian A B and C") {
    it("should load csv data from file") {
      val a = CustodianAConfig.process("/home/debasish/my-projects/reconciliation/recon/src/test/resources/DATA_CUSTODIAN_A.csv")
      a.size should equal(39)
      val b = CustodianBConfig.process("/home/debasish/my-projects/reconciliation/recon/src/test/resources/DATA_CUSTODIAN_B.txt")
      b.size should equal(184)
      val c = CustodianCConfig.process("/home/debasish/my-projects/reconciliation/recon/src/test/resources/DATA_CUSTODIAN_C.csv")
      c.size should equal(499)

      // List[String]
      val l = loadCustodianFetchValues(Seq(CollectionDef("rc", c.flatten.flatten), CollectionDef("ra", a.flatten.flatten), CollectionDef("rb", b.flatten.flatten)))
      l.size should equal(3)
      reconCustodianFetchValue(List("rc", "ra", "rb"), matchHeadAsSumOfRest).seq.foreach(println)
    }
  }
  **/

  describe("Custodian A B and C") {
    it("should load csv data from file") {
      val r = CustodianConfig.run(
        List(
          ("/home/debasish/my-projects/reconciliation/recon/src/test/resources/DATA_CUSTODIAN_C.csv", "C"),
          ("/home/debasish/my-projects/reconciliation/recon/src/test/resources/DATA_CUSTODIAN_A.csv", "A"),
          ("/home/debasish/my-projects/reconciliation/recon/src/test/resources/DATA_CUSTODIAN_B.txt", "B"))
      )
      type EitherEx[A] = Either[Throwable, A]
      loadCustodianFetchValues(
        r.seq.map {case (id, coll) => CollectionDef(id, coll.flatten.flatten)})
             .sequence[EitherEx, String]
             .fold(_ => sys.error("unexpected"), reconCustodianFetchValue(_, matchHeadAsSumOfRest).seq)
             .foreach(println)
    }
  }
}

