package net.debasishg.recon

import scalaz._
import Scalaz._

import Util._

object MatchFunctions {
  def match1on1[V](maybeVals: List[Option[List[V]]], tolerance: (V, V) => Boolean = (a: V, b: V) => a == b) = {

    def eqWithTolerance(l1: List[V], l2: List[V]) = {
      zipMap(l1, l2)(tolerance(_, _)).forall(_ == true)
    }

    val fl = maybeVals.flatten
    fl.size match {
      case l if l == maybeVals.size => fl forall (eqWithTolerance(_, fl.head))
      case _ => false
    }
  }

  def matchHeadAsSumOfRest[V: Monoid](maybeVals: List[Option[List[V]]], 
    tolerance: (V, V) => Boolean = (a: V, b: V) => a == b) = {
    val h = maybeVals.head
    val r = h map {hd =>
      val fl = maybeVals.tail.flatten
      if (fl.isEmpty) false
      else {
        val res = fl.tail.foldLeft(fl.head)(zipPlus(_, _))
        zipMap(res, hd)(tolerance) forall (_ == true)
      }
    }
    r getOrElse false
  }
}
