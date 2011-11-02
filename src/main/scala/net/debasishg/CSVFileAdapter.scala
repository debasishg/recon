package recon

import scalaz._
import Scalaz._
import java.io.File
import FileUtils._

trait CSVFieldXtractor extends Xtractor {
  // mapping a field to the position in the record: fieldName -> (0-based start position, length)
  val maps: Map[String, Int]

  // extract "field" from string "s" after looking up field position in "maps"
  def xtract(field: String, splits: Array[String]) = maps.get(field).map(s => splits(s))

  // extract "field" from string "s" after looking up field position in "maps" and apply "f" 
  def xtractAs[T](field: String, splits: Array[String])(implicit f: String => T) = 
    maps.get(field).map(s => f(splits(s)))
}
