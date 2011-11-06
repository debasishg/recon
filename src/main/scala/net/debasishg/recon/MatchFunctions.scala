package net.debasishg.recon

import scalaz._
import Scalaz._

object MatchFunctions {
  def match1on1[V](maybeVals: List[Option[List[V]]]) = {
    val fl = maybeVals.flatten
    fl.size match {
      case l if l == maybeVals.size => 
        splitVertical(fl).map(a => a.forall(_ == a.head)).forall(_ == true)
      case _ => false
    }
  }

  // def add2ListsElementwise[V: Monoid](l1: Option[List[V]], l2: Option[List[V]]) = {
    // (l1 |@| l2) {(x, y) => (x zip y) map (e => e._1 |+| e._2)}
  // }

  def add2ListsElementwise[V: Monoid](l1: List[V], l2: List[V]) = {
    (l1 zip l2) map (e => e._1 |+| e._2)
  }

  def splitVertical[V](l: List[List[V]]): List[List[V]] =
    if (l.forall(_ == Nil)) Nil 
    else l.map(_.head) :: splitVertical(l.map(_.tail))
  

  def matchHeadAsSumOfRest[V: Monoid](maybeVals: List[Option[List[V]]]) = {
    val h = maybeVals.head
    val r = h map {hd =>
      val fl = maybeVals.tail.flatten
      if (fl.isEmpty) false
      else {
        val res = fl.tail.foldLeft(fl.head)(add2ListsElementwise(_, _))
        res == hd
      }
    }
    r getOrElse false
  }
}
