package recon

import scalaz._
import Scalaz._
import java.io.File
import FileUtils._

trait FixedLengthFieldXtractor extends Xtractor {
  // mapping a field to the position in the record: fieldName -> (0-based start position, length)
  val maps: Map[String, (Int, Int)]

  // extract "field" from string "s" after looking up field position in "maps"
  def xtract(field: String, s: String) = maps.get(field).map(p => s.substring(p._1, p._1 + p._2))

  // extract "field" from string "s" after looking up field position in "maps" and apply "f" 
  def xtractAs[T](field: String, s: String)(implicit f: String => T) = 
    maps.get(field).map(p => f(s.substring(p._1, p._1 + p._2)))
}
