package recon

import com.twitter._
import util.{Future, FuturePool, Return, TimeoutException, Timer, JavaTimer}
import conversions.time._
import com.redis._
import serialization._
import java.util.concurrent.Executors

import scalaz._
import Scalaz._

import ReconEngine._

trait BalanceRecon {
  class RInt(orig: Int) {
    def toUSD(ccy: String) = ccy match {
      case "USD" => orig
      case _ => orig / 2
    }
  }
  implicit def enrichInt(i: Int) = new RInt(i)
  import Parse.Implicits.parseInt

  case class Balance(accountNo: String, date: org.joda.time.LocalDate, ccy: String, amount: Int)

  // typeclass instance for Balance
  implicit object BalanceProtocol extends ReconProtocol[Balance, String, Int] {
    def groupKey(b: Balance) = b.accountNo + b.date.toString
    def matchValue(b: Balance) = b.amount.toUSD(b.ccy)
  }

  def loadBalance(id: ReconId, balances: CollectionDef[Balance])(implicit clients: RedisClientPool) = 
    loadOneReconSet(id, balances)

  def loadBalances(ds: Map[ReconId, CollectionDef[Balance]])(implicit clients: RedisClientPool) =
    loadReconInputData(ds)

  def getBalance(id: ReconId)(implicit clients: RedisClientPool) = clients.withClient {client =>
    client.hgetall[String, Int](id)
  }

  def reconBalance(ids: Seq[ReconId], 
    fn: List[Option[Int]] => Boolean)(implicit clients: RedisClientPool) = 
    recon[String, Int](ids, fn)
}

object BalanceRecon extends BalanceRecon
