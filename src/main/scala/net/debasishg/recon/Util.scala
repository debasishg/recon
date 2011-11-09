package net.debasishg.recon

import scalaz._
import Scalaz._

import org.scala_tools.time.Imports._

object Util {
  def as[T, U](me: T)(implicit f: T => U) = f(me)

  def zipMap[A, B, C](l1: List[A], l2: List[B])(f: (A, B) => C) =
    l1 zip l2 map Function.tupled(f)

  def zipPlus[V: Monoid](l1: List[V], l2: List[V]) = zipMap(l1, l2)(_ |+| _)

  import Numeric.Implicits._
  def scale[T: Numeric](me: T, by: Int): Double = me.toDouble / math.pow(10, by)
}
