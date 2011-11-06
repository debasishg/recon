package net.debasishg.recon


trait ReconDef[ReconId, T] {
  val id: ReconId
  val values: Seq[T]
  val maybePred: Option[T => Boolean]
}

case class CollectionDef[ReconId, T](id: ReconId, values: Seq[T], maybePred: Option[T => Boolean] = None) extends ReconDef[ReconId, T]
