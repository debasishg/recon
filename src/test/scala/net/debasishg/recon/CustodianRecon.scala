package net.debasishg.recon

import com.twitter._
import util.{Future, FuturePool, Return, TimeoutException, Timer, JavaTimer}
import conversions.time._
import org.joda.time.LocalDate

import scalaz._
import Scalaz._

case class CustodianFetchValue(netAmount: Double,
  quantity: Double,
  security: String,
  transactionDate: LocalDate,
  transactionType: String)

trait CustodianReconEngine extends ReconEngine {
  type X = Double
  override val clientName = "australia-bank"

  // tolerance function for comparing values
  override def tolerancefn(x: Double, y: Double)(implicit ed: Equal[Double]) = math.abs(x - y) <= 1

  // typeclass instance for CustodianFetchValue
  implicit object CustodianDataProtocol extends ReconProtocol[CustodianFetchValue, Double] {
    def groupKey(v: CustodianFetchValue) = 
      v.security + ":" + v.transactionType + ":" + v.transactionDate.toString

    def matchValues(v: CustodianFetchValue) = Map("quantity" -> v.quantity, "netAmount" -> v.netAmount)
  }
}
