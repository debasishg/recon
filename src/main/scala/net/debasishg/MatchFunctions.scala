package recon

object MatchFunctions {
  def match1on1[V](maybeVals: List[Option[List[V]]]) = {
    val fl = maybeVals.flatten
    fl.size match {
      case l if l == maybeVals.size => 
        splitVertical(fl).map(a => a.forall(_ == a.head)).forall(_ == true)
      case _ => false
    }
  }

  def matchAsExpr[V](maybeVals: List[Option[List[V]]])(implicit fn: (List[V]) => Boolean) = {
    val fl = maybeVals.flatten
    fl.size match {
      case l if l == maybeVals.size => splitVertical(fl).forall(fn(_))
      case _ => false
    }
  }

  def splitVertical[V](l: List[List[V]]): List[List[V]] =
    if (l.forall(_ == Nil)) Nil 
    else l.map(_.head) :: splitVertical(l.map(_.tail))
}
