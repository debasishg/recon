package net.debasishg.recon

import org.joda.time.LocalDate

import com.redis._
import com.redis.serialization._
import Util._
import MatchFunctions._

import scalaz._
import Scalaz._

object ReconUtils {
  def fetchEntries[V](rc: RedisClient, client: String, runDate: LocalDate, which: ReconRez)(implicit m: Monoid[V], p: Parse[MatchList[V]], f: Format) = {
    rc.hgetall[String, MatchList[V]](client + ":" + runDate + ":" + which)
  }

  def fetchMatchEntries[V](rc: RedisClient, client: String, runDate: LocalDate)(implicit m: Monoid[V], p: Parse[MatchList[V]], f: Format) = fetchEntries[V](rc, client, runDate, Match)

  def fetchUnmatchEntries[V](rc: RedisClient, client: String, runDate: LocalDate)(implicit m: Monoid[V], p: Parse[MatchList[V]], f: Format) = fetchEntries[V](rc, client, runDate, Unmatch)

  def fetchBreakEntries[V](rc: RedisClient, client: String, runDate: LocalDate)(implicit m: Monoid[V], p: Parse[MatchList[V]], f: Format) = fetchEntries[V](rc, client, runDate, Break)
}

