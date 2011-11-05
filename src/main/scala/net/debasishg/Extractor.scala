package recon

import scalaz._
import Scalaz._
import java.io.File
import FileUtils._

trait Xtractor {
  def loop(fileName: String) = {
    val str = enumFile(new File(fileName), repeatHead) map (_.run)
    str.unsafePerformIO
  }
}

trait CSVFieldXtractor extends Xtractor {
  // mapping a field to the position in the record: fieldName -> (0-based start position, length)
  val maps: Map[String, Int]

  // extract "field" from string "s" after looking up field position in "maps"
  def xtract(field: String)(implicit splits: Array[String]) = maps.get(field).map(s => splits(s))

  // extract "field" from string "s" after looking up field position in "maps" and apply "f" 
  def xtractAs[T](field: String)(implicit f: String => T, splits: Array[String]) = 
    maps.get(field).map(s => f(splits(s)))
}

trait FixedLengthFieldXtractor extends Xtractor {
  // mapping a field to the position in the record: fieldName -> (0-based start position, length)
  val maps: Map[String, (Int, Int)]

  // extract "field" from string "s" after looking up field position in "maps"
  def xtract(field: String, s: String) = maps.get(field).map(p => s.substring(p._1, p._1 + p._2))

  // extract "field" from string "s" after looking up field position in "maps" and apply "f" 
  def xtractAs[T](field: String, s: String)(implicit f: String => T) = 
    maps.get(field).map(p => f(s.substring(p._1, p._1 + p._2)))
}
