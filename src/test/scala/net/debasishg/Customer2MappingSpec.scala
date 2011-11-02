package recon

import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import Customer2FilePosition._

@RunWith(classOf[JUnitRunner])
class Customer2FileMappingSpec extends Spec 
                with ShouldMatchers
                with BeforeAndAfterEach
                with BeforeAndAfterAll {

  describe("load data from file") {
    it("should load into hash") {
      val m = process("/home/debasish/my-projects/recon/src/test/resources/DATA_CUSTODIAN_B.txt")
      println(m.size)
      m.foreach(println)
    }
  }
}
