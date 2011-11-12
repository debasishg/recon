package net.debasishg.recon

import org.scalatest.{Spec, BeforeAndAfterEach, BeforeAndAfterAll}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith

import scalaz._
import Scalaz._

import Util._

@RunWith(classOf[JUnitRunner])
class MatchListSpec extends Spec 
                with ShouldMatchers
                with BeforeAndAfterEach
                with BeforeAndAfterAll {

  describe("match list") {
    it("should work for case 1") {
      val s1 = List(some(List(10, 20, 30)))
      val s2 = List(some(List(10, 20, 30)))
      (s1 |+| s2) should equal(List(some(List(20, 40, 60))))
    }
    it("should work for case 2") {
      val s1 = List(some(List(10, 20, 30)))
      val s2 = List(none)
      (s1 |+| s2) should equal(List(some(List(10, 20, 30))))
    }
    it("should work for case 3") {
      val s1: MatchList[Int] = List(none)
      val s2 = List(some(List(10, 20, 30)))
      (s1 |+| s2) should equal(List(some(List(10, 20, 30))))
    }
    it("should work for case 4") {
      val s1 = List(some(List(10, 20, 30)), some(List(1, 2, 3)))
      val s2 = List(some(List(10, 20, 30)), some(List(5, 6, 7)))
      (s1 |+| s2) should equal(List(some(List(20, 40, 60)), some(List(6, 8, 10))))
    }
    it("should work for case 5") {
      val s1 = List(some(List(10, 20, 30)), some(List(1, 2, 3)))
      val s2 = List(some(List(10, 20, 30)), none)
      (s1 |+| s2) should equal(List(some(List(20, 40, 60)), some(List(1, 2, 3))))
    }
  }
}

