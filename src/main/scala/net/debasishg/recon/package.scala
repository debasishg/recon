package net.debasishg

import scalaz._
import Scalaz._

import recon.Util._

package object recon {
  implicit def BigDecimalZero = new Zero[BigDecimal] {
    val zero = BigDecimal("0")
  }

  implicit def BigDecimalSemigroup =
    new Semigroup[BigDecimal] {
      def append(b1: BigDecimal, b2: => BigDecimal) =
        b1 + b2
    }

  implicit def MatchListSemigroup[V: Monoid] =
    new Semigroup[MatchList[V]] {
      def append(m1: MatchList[V], m2: => MatchList[V]) = {
        if (m1.size != m2.size) sys.error("Incompatible for append")
        (m1 zip m2) map {case (e1, e2) =>
          (e1, e2) match {
            case (f@Some(_), None) => f
            case (None, f@Some(_)) => f
            case (Some(l1), Some(l2)) if l1.size == l2.size => Some(zipPlus(l1, l2))
            case _ => None
          }
        }
      }
    }
}
