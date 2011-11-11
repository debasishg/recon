package net.debasishg.recon


trait ReconDef[T] {
  val id: String
  val values: Seq[T]
  val maybePred: Option[T => Boolean]
}

case class CollectionDef[T](id: String, values: Seq[T], maybePred: Option[T => Boolean] = None) extends ReconDef[T]
