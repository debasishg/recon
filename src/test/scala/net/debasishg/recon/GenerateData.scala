package net.debasishg.recon

import org.scala_tools.time.Imports._
import java.io._

object ReconDataGenerator {
  val now = DateTime.now.toLocalDate

  def genTxnValue(accountNo: String, numRecords: Int, numFiles: Int) = {
    val main = (1 to numRecords).map(100 * _)
    val mainBalanceList = main.map(i => Balance(accountNo, now, "USD", i))
    val sub = main.map(_ / numFiles)
    val subs = (1 to numFiles).map {i =>
      sub.map(Balance(accountNo, now, "USD", _))
    }
    (mainBalanceList, subs)
  }

  def generateDataForMultipleAccounts = {
    // 1 main list and 2 sub lists
    // main list = sublist1 + sublist2
    var mainList = new collection.mutable.ListBuffer[Seq[Balance]]
    var subList1 = new collection.mutable.ListBuffer[Balance]
    var subList2 = new collection.mutable.ListBuffer[Balance]

    // 1000 accounts
    (1 to 1000).foreach {a =>
      // 100 records per account in each list
      // 2 sub lists
      val (m, s) = genTxnValue("acc" + a, 200, 2)
      mainList += m
      val ss = s.splitAt(1)
      ss._1.flatten.map(subList1 += _)
      ss._2.flatten.map(subList2 += _)
    }

    /**
    printToFile(new File("/home/debasish/balance/main.csv"))(p => {
      mainList.flatten.toList.map(b => b.accountNo + "," + b.amount).foreach(p.println)
    })
    printToFile(new File("/home/debasish/balance/sub1.csv"))(p => {
      subList1.toList.map(b => b.accountNo + "," + b.amount).foreach(p.println)
    })
    printToFile(new File("/home/debasish/balance/sub2.csv"))(p => {
      subList2.toList.map(b => b.accountNo + "," + b.amount).foreach(p.println)
    })
    **/

    // each list will have 100000 records
    (mainList.flatten.toList, subList1.toList, subList2.toList)
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
}
