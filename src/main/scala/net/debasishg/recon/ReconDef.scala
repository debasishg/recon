package net.debasishg.recon

import scalaz._
import Scalaz._

trait ReconDef[T] {
  val id: String
  val values: Seq[T]
  val maybePred: Option[T => Boolean]
}

case class CollectionDef[T](id: String, values: Seq[T], maybePred: Option[T => Boolean] = None) extends ReconDef[T]

import MatchFunctions._
case class ReconResult[K, V: Monoid](field: K, matched: List[Option[List[V]]], result: ReconRez)
