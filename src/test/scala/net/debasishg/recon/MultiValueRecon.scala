package net.debasishg.recon

import com.twitter._
import util.{Future, FuturePool, Return, TimeoutException, Timer, JavaTimer}
import conversions.time._
import com.redis._
import serialization._
import java.util.concurrent.Executors

case class TradeData(accountNo: String, 
  tradeDate: org.joda.time.LocalDate, security: String, quantity: Int, amount: Int)

trait TradeDataRecon {
  lazy val engine = new ReconEngine { 
    type X = Int
  }

  import engine._

  import Parse.Implicits.parseInt

  // typeclass instance for TradeData
  implicit object TradeDataProtocol extends ReconProtocol[TradeData, Int] {
    def groupKey(t: TradeData) = t.accountNo + t.tradeDate.toString + t.security
    def matchValues(t: TradeData) = Map("quantity" -> t.quantity, "amount" -> t.amount)
  }

  def loadTradeData(trades: CollectionDef[TradeData])
    (implicit clients: RedisClientPool) = loadOneReconSet(trades)

  def loadAllTradeData(ds: Seq[CollectionDef[TradeData]])
    (implicit clients: RedisClientPool) = loadReconInputData(ds)

  def reconTradeData(ids: Seq[String], 
    fn: (List[Option[List[Int]]], (Int, Int) => Boolean) => MatchFunctions.ReconRez)
    (implicit clients: RedisClientPool) = 
    recon[Int](ids, fn)
}

object TradeDataRecon extends TradeDataRecon
