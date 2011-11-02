package recon

import scalaz._
import Scalaz._
import java.io.File
import FileUtils._

trait Xtractor {
  // mapping a field to the position in the record: fieldName -> (0-based start position, length)
  val maps: Map[String, (Int, Int)]

  // extract "field" from string "s" after looking up field position in "maps"
  def xtract(field: String, s: String) = maps.get(field).map(p => s.substring(p._1, p._1 + p._2))

  // extract "field" from string "s" after looking up field position in "maps" and apply "f" 
  def xtractAs[T](field: String, s: String, f: String => T) = 
    maps.get(field).map(p => f(s.substring(p._1, p._1 + p._2)))

  // loop through the file using the iteratee based interface and generate T from extracted fields
  def loop[T](fileName: String, apply: String => T) = {
    val str = enumFile(new File(fileName), repeatHead) map (_.run)
    str.unsafePerformIO.map(_.map(apply(_)))
  }
}
