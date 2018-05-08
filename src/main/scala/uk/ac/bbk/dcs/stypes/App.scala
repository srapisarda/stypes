package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.Variable

/**
  * Created by salvo on 08/05/2018.
  */
object App {

  /** Main program method */
  def main(args: Array[String]): Unit = {
    App.executeRewriting(args(0), args(1), args(2)  )
  }

  private def executeRewriting (cqPath:String, gmlPath:String, ontologyPath:String) = {
    val ontology = ReWriter.getOntology(ontologyPath)
    val decomposedQuery: (TreeDecomposition, List[Variable]) =
      TreeDecomposition.getTreeDecomposition( gmlPath, cqPath)
    val datalog = new ReWriter(ontology).rewrite(decomposedQuery, true)
    printDatalog(datalog)
  }

  private def printDatalog(datalog: List[Clause]): Unit =
    println(s"${datalog.mkString(".\n")}.".replaceAll("""\[\d+\]""", ""))
}
