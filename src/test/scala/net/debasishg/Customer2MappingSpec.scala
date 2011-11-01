package recon

import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scala_tools.time.Imports._

import Mappings._

@RunWith(classOf[JUnitRunner])
class Customer2FileMappingSpec extends Spec 
                with ShouldMatchers
                with BeforeAndAfterEach
                with BeforeAndAfterAll {

  describe("load data from file") {
    it("should load into hash") {
      val m = process("/Users/debasishghosh/projects/recon/src/test/resources/DATA_CUSTODIAN_B.txt")
      println(m.size)
      m.foreach(println)
    }
  }
}
