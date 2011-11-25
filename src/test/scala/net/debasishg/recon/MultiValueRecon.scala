package net.debasishg.recon

import Util._
case class TradeData(accountNo: String, 
  tradeDate: org.joda.time.LocalDate, security: String, quantity: Int, amount: Int)

trait TradeDataReconEngine extends ReconEngine[TradeData, Int] {
  type X = Int
  override val clientName = "dummy"
  override val runDate = now

  // typeclass instance for TradeData
  implicit object TradeDataProtocol extends ReconProtocol[TradeData, Int] {
    def groupKey(t: TradeData) = t.accountNo + t.tradeDate.toString + t.security
    def matchValues(t: TradeData) = Map("quantity" -> t.quantity, "amount" -> t.amount)
  }
}

object TradeDataReconEngine extends TradeDataReconEngine
