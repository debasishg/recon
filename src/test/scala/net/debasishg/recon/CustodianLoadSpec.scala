package net.debasishg.recon

import scala.collection.parallel.ParSet
import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

@RunWith(classOf[JUnitRunner])
class CustodianLoadSpec extends Spec 
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

