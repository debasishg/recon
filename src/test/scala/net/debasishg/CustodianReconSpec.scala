package recon

import scala.collection.parallel.ParSet
import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scala_tools.time.Imports._

@RunWith(classOf[JUnitRunner])
class CustodianReconSpec extends Spec 
                         with ShouldMatchers
                         with BeforeAndAfterEach
                         with BeforeAndAfterAll {

  describe("Custodian A") {
    import CustodianAConfig._
    it("should load csv data from file") {
      val m = process("/Users/debasishghosh/projects/recon/src/test/resources/DATA_CUSTODIAN_A.csv")
      m.size should equal(39)
    }
  }

  describe("Custodian B") {
    import CustodianBConfig._
    it("should load csv data from file") {
      val m = process("/Users/debasishghosh/projects/recon/src/test/resources/DATA_CUSTODIAN_B.txt")
      m.size should equal(184)
    }
  }

  describe("Custodian C") {
    import CustodianCConfig._
    it("should load csv data from file") {
      val m = process("/Users/debasishghosh/projects/recon/src/test/resources/DATA_CUSTODIAN_C.csv")
      m.size should equal(499)
    }
  }
}

