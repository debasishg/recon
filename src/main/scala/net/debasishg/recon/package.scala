package net.debasishg

import scalaz._
import Scalaz._

package object recon {
  implicit def BigDecimalZero = new Zero[BigDecimal] {
    val zero = BigDecimal("0")
  }

  implicit def BigDecimalSemigroup =
    new Semigroup[BigDecimal] {
      def append(b1: BigDecimal, b2: => BigDecimal) =
        b1 + b2
    }
}
