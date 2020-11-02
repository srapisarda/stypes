package uk.ac.bbk.dcs.stypes

object TestUtil {
  def printDatalog(datalog: List[Clause]): Unit =
    println(s"${datalog.mkString(".\n")}.".replaceAll("""\[\d+\]""", ""))
}
