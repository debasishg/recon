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

  describe("test") {
    it ("should") {
      type EitherInt[A] = Either[Int, A]

      def fs(f: Seq[String]) = (f map (_.length)).some
      def bs(f: Seq[Int]) = (f map (_ * 2)).some
      def li(ls: Seq[Int]) = {
        val p = ls map {l =>
          if (l <= 2) Left(100) else Right(l.toString)
        }
        p.sequence[EitherInt, String]
      }
      val list = List("abcd", "a", "bcd", "efghi")
      val lit = List("a", "bcd", "efghi")
      // println(li(fs(list).get))
      // println(fs(list) map {l => li(l)})

      val y =
      for {
        f <- fs(lit)
        b <- bs(f)
      } yield(li(b))
      println(y)

      def foo(s: String) = {i: Int => "debasish" + s + i.toString}
      def bar = {i: Int => i + 20}
      def baz = {i: Int => if (i == 0) false else true}
      val x =
        for {
          f <- foo("ghosh")
          b <- bar
          c <- baz
        } yield (f, b, c)
      println(x(20))
    }
  }

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

