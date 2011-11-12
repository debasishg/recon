package net.debasishg.recon

import scalaz._
import Scalaz._

import Util._

object MatchFunctions {
  sealed trait ReconRez
  case object Match extends ReconRez
  case object Unmatch extends ReconRez
  case object Break extends ReconRez

  def match1on1[V](maybeVals: MatchList[V], tolerance: (V, V) => Boolean = (a: V, b: V) => a == b) = {

    def eqWithTolerance(l1: List[V], l2: List[V]) = {
      zipMap(l1, l2)(tolerance(_, _)).forall(_ == true)
    }

    val fl = maybeVals.flatten
    fl.size match {
      case l if l == maybeVals.size => (fl forall (eqWithTolerance(_, fl.head))) match {
        case true => Match
        case _ => Unmatch
      }
      case _ => Break
    }
  }

  def matchHeadAsSumOfRest[V: Monoid](maybeVals: MatchList[V], 
    tolerance: (V, V) => Boolean = (a: V, b: V) => a == b) = {
    val h = maybeVals.head
    val r = h map {hd =>
      val fl = maybeVals.tail.flatten
      if (fl.isEmpty) Break
      else {
        val res = fl.tail.foldLeft(fl.head)(zipPlus(_, _))
        (zipMap(res, hd)(tolerance) forall (_ == true)) match {
          case true => Match
          case _ => Unmatch
        }
      }
    }
    r getOrElse Break
  }
}
