package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.Variable

/**
  * Created by salvo on 08/05/2018.
  */
object App {

  /** Main program method */
  def main(args: Array[String]): Unit = {
    val splitMode = if (args.length > 3 && args(3).equalsIgnoreCase("leaf")) Leaf else Centroid
    App.executeRewriting(args(0), args(1), args(2), splitMode)
  }

  private def executeRewriting(cqPath: String, gmlPath: String, ontologyPath: String, splitMode: SplitMode = Centroid) = {
    val ontology = ReWriter.getOntology(ontologyPath)
    val decomposedQuery: (TreeDecomposition, List[Variable]) =
      TreeDecomposition.getTreeDecomposition(gmlPath, cqPath)
    val datalog =
      new ReWriter(ontology).rewrite(decomposedQuery, addAdditionalRules = true, splitMode = splitMode)
    printDatalog(datalog)
  }

  private def printDatalog(datalog: List[Clause]): Unit =
    println(datalog.mkString("\n"))
}
