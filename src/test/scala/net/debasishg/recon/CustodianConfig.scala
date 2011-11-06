package net.debasishg.recon

import scalaz._
import Scalaz._

/**
 * @todo
 * 1. trim space for fixed length records => done
 * 2. take care of date formatting
 * 3. uniformity of transaction type => hard coded now
 * 4. handling scale => hard coded now
 */
trait CustodianBConfig extends FixedLengthFieldXtractor {
  val maps = // 0-based start position 
    Map("effectiveValue"       -> (276, 18), 
        "effectiveValueDc"     -> (294, 1), 
        "longQuantity"         -> (112, 12), 
        "shortQuantity"        -> (125, 12), 
        "security"             -> (66, 6), 
        "transactionDate"      -> (295, 8), 
        "transactionType"      -> (110, 1))

  def process(fileName: String) = {
    implicit val f = (s: String) => s.toDouble

    val s = loop(fileName)
    s.map(_.map {str =>

      // derive netAmount
      val effectiveValue = xtractAs("effectiveValue", str) 
      val effectiveValueDc = xtract("effectiveValueDc", str) 

      val netAmount = (effectiveValue |@| effectiveValueDc) {(v, c) =>
        c match {
          case "C" => v/100
          case _ => (-1) * (v/100)
        }
      }

      // derive quantity
      val transactionType = xtract("transactionType", str)
      val longQuantity = xtractAs("longQuantity", str) 
      val shortQuantity = xtractAs("shortQuantity", str) 

      val quantity = (transactionType |@| longQuantity |@| shortQuantity) {(t, l, s) =>
        t match {
          case "B" => (l/100)
          case _ => (-1) * (s/100)
        }
      }

      netAmount                         |@| 
      quantity                          |@| 
      xtract("security", str)           |@|
      xtract("transactionDate", str)    |@|
      transactionType apply CustodianFetchValue.apply
    })
  }
}

object CustodianBConfig extends CustodianBConfig

trait CustodianAConfig extends CSVFieldXtractor {
  val maps = // 0-based start position 
    Map("netAmount"          -> 17,
        "quantity"           -> 11,
        "security"           -> 6,
        "transactionDate"    -> 4,
        "transactionType"    -> 8)

  def process(fileName: String) = {
    implicit val f = (s: String) => s.toDouble

    val s = loop(fileName)
    s.map(_.map {str =>
      implicit val splits: Array[String] = str.split(",") 
      xtractAs("netAmount")        |@|
      xtractAs("quantity")         |@|
      xtract("security")           |@|
      xtract("transactionDate")    |@|
      xtract("transactionType") apply CustodianFetchValue.apply
    })
  }
}

object CustodianAConfig extends CustodianAConfig

trait CustodianCConfig extends CSVFieldXtractor {
  val maps = // 0-based start position 
    Map("brokerage"          -> 30,
        "gstAmount"          -> 29,
        "netProceedAmount"   -> 27,
        "quantity"           -> 23,
        "security"           -> 11,
        "transactionDate"    -> 6,
        "transactionType"    -> 1)

  def process(fileName: String) = {
    implicit val f = (s: String) => s.toDouble

    val s = loop(fileName)
    s.map(_.map {str =>
      implicit val splits = str.split(",") 
      val brokerage = xtractAs("brokerage") 
      val gstAmount = xtractAs("gstAmount")
      val netProceedAmount = xtractAs("netProceedAmount")

      val netAmount = (netProceedAmount |@| brokerage |@| gstAmount) {(a, b, g) => (a - (b + g))}

      netAmount                    |@|
      xtractAs("quantity")         |@|
      xtract("security")           |@|
      xtract("transactionDate")    |@|
      xtract("transactionType") apply CustodianFetchValue.apply
    })
  }
}

object CustodianCConfig extends CustodianCConfig
