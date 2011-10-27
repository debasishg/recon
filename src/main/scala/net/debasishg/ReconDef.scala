package recon


trait ReconDef[T] {
  val values: Seq[T]
  val maybePred: Option[T => Boolean]
}

case class CollectionDef[T](values: Seq[T], maybePred: Option[T => Boolean] = None) extends ReconDef[T]
