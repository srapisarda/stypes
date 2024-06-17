package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.scalatest.FunSpec

class CentroidDecompositionTest extends FunSpec {
  describe("Centroid Decomposition Test") {

    it("should return the centroid of a tree from q-thesis-2") {
      val vertex =  TreeDecomposition.getTreeDecomposition(
          "src/test/resources/benchmark/Lines/gml/q-thesis-2.gml",
          "src/test/resources/benchmark/Lines/queries/q-thesis-2.cq")._1

      val centroid = CentroidDecomposition.getCentroid(vertex)

      println("Centroid: " + centroid)
      assert(centroid.getRoot.variables ==
        Set(
          DefaultTermFactory.instance.createVariable("x3"),
          DefaultTermFactory.instance.createVariable("x4")))
    }

    it("should return the centroid of a tree from q-thesis-3-vertex") {
      val vertex =  TreeDecomposition.getTreeDecomposition(
        "src/test/resources/benchmark/Lines/gml/q-thesis-3-vertex.gml",
        "src/test/resources/benchmark/Lines/queries/q-thesis-3-vertex.cq")._1

      val centroid = CentroidDecomposition.getCentroid(vertex)

      println("Centroid: " + centroid)
      assert(centroid.getRoot.variables ==
        Set(
          DefaultTermFactory.instance.createVariable("y"),
          DefaultTermFactory.instance.createVariable("z")))
    }

    it("should return the centroid of a tree from q-thesis-3-vertex") {
      val vertex =  TreeDecomposition.getTreeDecomposition(
        "src/test/resources/benchmark/Lines/gml/q-thesis-3-vertex.gml",
        "src/test/resources/benchmark/Lines/queries/q-thesis-3-vertex.cq")._1

      val centroid = CentroidDecomposition.getCentroid(vertex)

      println("Centroid: " + centroid)
      assert(centroid.getRoot.variables ==
        Set(
          DefaultTermFactory.instance.createVariable("y"),
          DefaultTermFactory.instance.createVariable("z")))
    }



  }
}
