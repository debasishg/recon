package net.debasishg.recon

import Util._

case class Balance(accountNo: String, date: org.joda.time.LocalDate, ccy: String, amount: Int)
trait BalanceReconEngine extends ReconEngine {
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
