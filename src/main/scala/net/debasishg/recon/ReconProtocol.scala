package net.debasishg.recon

import scalaz._
import Scalaz._

// typeclass for recon protocol
// this is the view of a single record from the data source
// for the same set of groupKey, the data source may have multiple values
// they will be combined using the append operation of the Monoid
abstract class ReconProtocol[T, K, V: Monoid] {
  // keys on which grouping has to be done for loading recon records
  def groupKey(t: T): K

  // these need to be matched as part of recon process
  def matchValues(t: T): Map[String, V]
}
