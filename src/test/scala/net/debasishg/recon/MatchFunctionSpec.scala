package net.debasishg.recon

import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import scalaz._
import Scalaz._

import MatchFunctions._

@RunWith(classOf[JUnitRunner])
class MatchFunctionSpec extends Spec 
                with ShouldMatchers
                with BeforeAndAfterEach
                with BeforeAndAfterAll {

  describe("match function") {
    it("should work") {
      val s1 = List(some(List(10, 20, 30)), some(List(5, 10, 15)), some(List(5, 10, 15)), some(List(0, 0, 0)))
      matchHeadAsSumOfRest(s1) should equal(Match)
      val s2 = List(some(List(10, 20, 30)), some(List(5, 10, 15)), none[List[Int]], some(List(5, 10, 15)), some(List(0, 0, 0)))
      matchHeadAsSumOfRest(s2) should equal(Match)
      val s3 = List(some(List(10, 20, 30)), none[List[Int]], some(List(5, 10, 15)), none[List[Int]], some(List(5, 10, 15)), some(List(0, 0, 0)))
      matchHeadAsSumOfRest(s3) should equal(Match)
      val s4 = List(none[List[Int]], some(List(10, 20)), none[List[Int]])
      matchHeadAsSumOfRest(s4) should equal(Break)
      val s5 = List(none[List[Int]], none[List[Int]], some(List(10, 20)), none[List[Int]])
      matchHeadAsSumOfRest(s5) should equal(Break)
      val s6 = List(some(List(10, 20)), some(List(10, 20)), none[List[Int]])
      matchHeadAsSumOfRest(s6) should equal(Match)
      val s7 = List(some(List(10, 20)), none[List[Int]], none[List[Int]])
      matchHeadAsSumOfRest(s7) should equal(Break)
    }
  }
}

