package net.debasishg.recon

import com.twitter._
import util.{Future, FuturePool, Return, TimeoutException, Timer, JavaTimer}
import conversions.time._
import com.redis._
import serialization._
import java.util.concurrent.Executors
import org.joda.time.LocalDate
import org.scala_tools.time.Imports._
import Util._

import scalaz._
import Scalaz._

case class CustodianFetchValue(netAmount: Double,
  quantity: Double,
  security: String,
  transactionDate: LocalDate,
  transactionType: String)

trait CustodianRecon {
  lazy val engine = new ReconEngine { 
    type X = Double
    override val clientName = "australia-bank"

    // tolerance function for comparing values
    override def tolerancefn(x: Double, y: Double)(implicit ed: Equal[Double]) = 
      if (math.abs(x - y) <= 1) true else false
  }

  import engine._

  import Parse.Implicits.parseDouble

  // typeclass instance for CustodianFetchValue
  implicit object CustodianDataProtocol extends ReconProtocol[CustodianFetchValue, Double] {
    def groupKey(v: CustodianFetchValue) = 
      v.security + ":" + v.transactionType + ":" + v.transactionDate.toString
    def matchValues(v: CustodianFetchValue) = Map("quantity" -> v.quantity, "netAmount" -> v.netAmount)
  }

  def loadCustodianFetchValue(values: CollectionDef[CustodianFetchValue], runDate: LocalDate = now)
    (implicit clients: RedisClientPool) = loadOneReconSet(values, runDate)

  def loadCustodianFetchValues(ds: Seq[CollectionDef[CustodianFetchValue]], runDate: LocalDate = now)
    (implicit clients: RedisClientPool) = loadReconInputData(ds, runDate)

  def reconCustodianFetchValue(ids: Seq[String], 
    fn: (List[Option[List[Double]]], (Double, Double) => Boolean) => MatchFunctions.ReconRez, runDate: LocalDate = now)
    (implicit clients: RedisClientPool) = 
    recon[Double](ids, fn, runDate)
}

object CustodianRecon extends CustodianRecon
