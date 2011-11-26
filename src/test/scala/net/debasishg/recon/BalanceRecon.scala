package net.debasishg.recon

import scalaz._
import Scalaz._

import Util._

case class Balance(accountNo: String, date: org.joda.time.LocalDate, ccy: String, amount: Int)
trait BalanceReconEngine extends ReconEngine[Balance, Int] {
  type X = Int
  override val clientName = "dummy"
  override val runDate = now

  class RInt(orig: Int) {
    def toUSD(ccy: String) = ccy match {
      case "USD" => orig
      case _ => orig / 2
    }
  }
  implicit def enrichInt(i: Int) = new RInt(i)

  // typeclass instance for Balance
  implicit object BalanceProtocol extends ReconProtocol[Balance, Int] {
    def groupKey(b: Balance) = b.accountNo + b.date.toString
    def matchValues(b: Balance) = Map("amount" -> b.amount.toUSD(b.ccy))
  }
}

object BalanceReconEngine extends BalanceReconEngine

trait BalanceConfig extends CSVFieldXtractor with ReconSource[Balance] {
  val maps = // 0-based start position 
    Map("accountNo" -> 0, "amount" -> 1)

  def processSingle(str: String) = {
    val f = (s: String) => s.toInt
    implicit val splits: Array[String] = str split ","  

    xtract("accountNo") |@| 
    (now.some) |@| 
    ("USD".some) |@| 
    (xtract("amount") map f) apply Balance.apply
  }
}

trait BalanceMainConfig extends BalanceConfig {
  override val id = "b-main"
}
trait BalanceSub1Config extends BalanceConfig {
  override val id = "b-sub1"
}
trait BalanceSub2Config extends BalanceConfig {
  override val id = "b-sub2"
}

object BalanceMainConfig extends BalanceMainConfig
object BalanceSub1Config extends BalanceSub1Config
object BalanceSub2Config extends BalanceSub2Config

