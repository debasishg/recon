package recon

import scalaz._
import Scalaz._
import IterV._
import java.io.File
import FileUtils._

case class Customer2FileMapping(effectiveValue: BigDecimal, effectiveValueDc: String, 
  longQuantity: BigDecimal, shortQuantity: BigDecimal, security: String, 
  transactionDate: String, transactionType: String) {
  val netAmount = effectiveValueDc match {
    case "C" => effectiveValue
    case _ => (-1) * effectiveValue
  }

  val quantity = transactionType match {
    case "B" => longQuantity
    case _ => (-1) * shortQuantity
  }

  final val positions = Map(1 -> (277, 18), 2 -> (295, 1), 3 -> (113, 12), 4 -> (126, 12), 5 -> (67, 6), 6 -> (296, 8), 7 -> (111, 1))
}

object Mappings {
  def process(name: String) = {
    val str = enumFile(new File(name), repeatHead) map (_.run)
    str.unsafePerformIO.map(_.map(s => 
      Customer2FileMapping(BigDecimal(s.substring(276, 276 + 18)), 
        s.substring(294, 294 + 1), 
        BigDecimal(s.substring(112, 112 + 12)), 
        BigDecimal(s.substring(125, 125 + 12)), 
        s.substring(66, 66 + 6), 
        s.substring(295, 295 + 7), 
        s.substring(110, 110 + 1))))
  }
}

