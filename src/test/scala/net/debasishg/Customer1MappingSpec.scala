package recon

import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import Customer1FilePosition._

@RunWith(classOf[JUnitRunner])
class Customer1FileMappingSpec extends Spec 
                with ShouldMatchers
                with BeforeAndAfterEach
                with BeforeAndAfterAll {

  describe("load data from file") {
    it("should load into hash") {
      val m = process("/Users/debasishghosh/projects/recon/src/test/resources/DATA_CUSTODIAN_A.csv")
      println(m.size)
      m.map(e => e.get.get.security).toSet.toList.sorted.foreach(println)
    }
  }
}
