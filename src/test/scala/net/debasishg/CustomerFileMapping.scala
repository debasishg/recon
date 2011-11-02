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

object Customer2FilePosition extends FixedLengthFieldXtractor {
  val maps = // 0-based start position 
    Map("effectiveValue"       -> (276, 18), 
        "effectiveValueDc"     -> (294, 1), 
        "longQuantity"         -> (112, 12), 
        "shortQuantity"        -> (125, 12), 
        "security"             -> (66, 6), 
        "transactionDate"      -> (295, 8), 
        "transactionType"      -> (110, 1))

  def process(fileName: String) = {
    implicit val f = (s: String) => BigDecimal(s)
    loop(fileName, 
      (s: String) => 
        xtractAs("effectiveValue", s)     |@|
        xtract("effectiveValueDc", s)     |@|
        xtractAs("longQuantity", s)       |@|
        xtractAs("shortQuantity", s)      |@|
        xtract("security", s)             |@|
        xtract("transactionDate", s)      |@|
        xtract("transactionType", s) apply Customer2FileMapping.apply)
  }
}

case class Customer1FileMapping(netAmount: BigDecimal, quantity: BigDecimal, 
  security: String, transactionDate: String, transactionType: String)

object Customer1FilePosition extends CSVFieldXtractor {
  val maps = // 0-based start position 
    Map("netAmount"          -> 17,
        "quantity"           -> 11,
        "security"           -> 6,
        "transactionDate"    -> 4,
        "transactionType"    -> 8)

  def process(fileName: String) = {
    implicit val f = (s: String) => BigDecimal(s)
    loop(fileName, 
      (s: String) => {
        val splits = s.split(",") 
        xtractAs("netAmount", splits)        |@|
        xtractAs("quantity", splits)         |@|
        xtract("security", splits)           |@|
        xtract("transactionDate", splits)    |@|
        xtract("transactionType", splits) apply Customer1FileMapping.apply
      })
  }
}

case class Customer3FileMapping(brokerage: BigDecimal, gstAmount: BigDecimal, 
  netProceedAmount: BigDecimal, quantity: BigDecimal, security: String, transactionDate: String, 
  transactionType: String) {
  val netAmount = netProceedAmount - (brokerage + gstAmount)
}

object Customer3FilePosition extends CSVFieldXtractor {
  val maps = // 0-based start position 
    Map("brokerage"          -> 30,
        "gstAmount"          -> 29,
        "netProceedAmount"   -> 27,
        "quantity"           -> 23,
        "security"           -> 11,
        "transactionDate"    -> 6,
        "transactionType"    -> 1)

  def process(fileName: String) = {
    implicit val f = (s: String) => BigDecimal(s)
    loop(fileName, 
      (s: String) => {
        val splits = s.split(",") 
        xtractAs("brokerage", splits)        |@|
        xtractAs("gstAmount", splits)        |@|
        xtractAs("netProceedAmount", splits) |@|
        xtractAs("quantity", splits)         |@|
        xtract("security", splits)           |@|
        xtract("transactionDate", splits)    |@|
        xtract("transactionType", splits) apply Customer3FileMapping.apply
      })
  }
}
