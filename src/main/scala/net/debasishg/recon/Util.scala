package net.debasishg.recon

import scalaz._
import Scalaz._

object Util {
  def zipMap[A, B, C](l1: List[A], l2: List[B])(f: (A, B) => C) =
    l1 zip l2 map Function.tupled(f)

  def zipPlus[V: Monoid](l1: List[V], l2: List[V]) = zipMap(l1, l2)(_ |+| _)

  import Numeric.Implicits._
  def scale[T: Numeric](me: T, by: Int): Double = me.toDouble / math.pow(10, by)

  type MatchList[V] = List[Option[List[V]]]
}
