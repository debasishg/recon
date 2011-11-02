package recon

import scalaz._
import Scalaz._
import java.io.File
import FileUtils._

trait Xtractor {
  // loop through the file using the iteratee based interface and generate T from extracted fields
  def loop[T](fileName: String, apply: String => T) = {
    val str = enumFile(new File(fileName), repeatHead) map (_.run)
    str.unsafePerformIO.map(_.map(apply(_)))
  }
}
