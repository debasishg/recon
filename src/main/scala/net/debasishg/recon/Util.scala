package net.debasishg.recon

import scalaz._
import Scalaz._
import Numeric.Implicits._
import sjson.json.Format
import sjson.json.DefaultProtocol._
import sjson.json.JsonSerialization._
import org.scala_tools.time.Imports._

object Util {
  val now = DateTime.now.toLocalDate

  def zipMap[A, B, C](l1: List[A], l2: List[B])(f: (A, B) => C): List[C] =
    l1 zip l2 map Function.tupled(f)

  def zipPlus[V: Monoid](l1: List[V], l2: List[V]) = zipMap(l1, l2)(_ |+| _)

  def scale[T: Numeric](me: T, by: Int): Double = me.toDouble / math.pow(10, by)

  type MatchList[V] = List[Option[List[V]]]

  def serializeMatchList[V](l: MatchList[V])
    (implicit m: Format[MatchList[V]]) = tobinary(l)

  def deSerializeMatchList[V](bytes: Array[Byte]) 
    (implicit m: Format[MatchList[V]]) = {
    try {
      frombinary[MatchList[V]](bytes)
    } catch { 
      case th: Throwable =>
        println("***** from deserialize: " + new String(bytes, "UTF-8"))
        sys.error(th.getMessage)
    }
  }
}
