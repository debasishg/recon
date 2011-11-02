package recon

import scalaz._
import Scalaz._
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
}

object Customer2FilePosition extends Xtractor {
  val maps = // 0-based start position 
    Map("effectiveValue" -> (276, 18), 
        "effectiveValueDc" -> (294, 1), 
        "longQuantity" -> (112, 12), 
        "shortQuantity" -> (125, 12), 
        "security" -> (66, 6), 
        "transactionDate" -> (295, 8), 
        "transactionType" -> (110, 1))

  def process(name: String) = {
    val f = (s: String) => BigDecimal(s)
    loop(name, 
      (s: String) => 
        xtractAs("effectiveValue", s, f)     |@|
        xtract("effectiveValueDc", s)        |@|
        xtractAs("longQuantity", s, f)       |@|
        xtractAs("shortQuantity", s, f)      |@|
        xtract("security", s)                |@|
        xtract("transactionDate", s)         |@|
        xtract("transactionType", s) apply Customer2FileMapping.apply)
  }
}
