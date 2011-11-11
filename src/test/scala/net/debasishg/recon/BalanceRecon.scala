package net.debasishg.recon

import com.twitter._
import util.{Future, FuturePool, Return, TimeoutException, Timer, JavaTimer}
import conversions.time._
import com.redis._
import serialization._
import java.util.concurrent.Executors

import scalaz._
import Scalaz._

case class Balance(accountNo: String, date: org.joda.time.LocalDate, ccy: String, amount: Int)
trait BalanceRecon {
  lazy val engine = new ReconEngine { 
    type ReconId = String 
    type X = Int
  }

  import engine._

  class RInt(orig: Int) {
    def toUSD(ccy: String) = ccy match {
      case "USD" => orig
      case _ => orig / 2
    }
  }
  implicit def enrichInt(i: Int) = new RInt(i)
  import Parse.Implicits.parseInt

  // typeclass instance for Balance
  implicit object BalanceProtocol extends ReconProtocol[Balance, String, Int] {
    def groupKey(b: Balance) = b.accountNo + b.date.toString
    def matchValues(b: Balance) = Map("amount" -> b.amount.toUSD(b.ccy))
  }

  def loadBalance(balances: CollectionDef[Balance])(implicit clients: RedisClientPool) = 
    loadOneReconSet(balances)

  def loadBalances(ds: Seq[CollectionDef[Balance]])(implicit clients: RedisClientPool) =
    loadReconInputData(ds)

  def getBalance(id: ReconId)(implicit clients: RedisClientPool) = clients.withClient {client =>
    client.hgetall[String, Int](id)
  }

  def reconBalance(ids: Seq[ReconId], fn: (List[Option[List[Int]]], (Int, Int) => Boolean) => MatchFunctions.ReconRez)
    (implicit clients: RedisClientPool) = 
    recon[String, Int](ids, fn)
}

object BalanceRecon extends BalanceRecon
