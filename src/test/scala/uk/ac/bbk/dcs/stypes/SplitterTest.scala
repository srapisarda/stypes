package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.scalatest.FunSpec

import scala.collection.JavaConverters._
import scala.reflect.io.File

class SplitterTest extends FunSpec {

  private val pathToLine = "src/test/resources/benchmark/Lines"
  private val pathToExpectedSplitterLogs = "src/test/resources/splitter";

  describe("Splitter") {

    it("should return all terms") {
      val terms = getTerms(List("x6", "x7", "x2", "x3", "x0", "x4", "x1", "x5"))
      val treeDecomposition = TreeDecomposition.
        getTreeDecomposition(s"$pathToLine/gml/q-thesis-1.multivar.gml", s"$pathToLine/queries/q-thesis-1.cq")
      val splitter = Splitter(treeDecomposition._1)


      println("Splitter ----------")
      println(println(splitter.flattenLog().mkString("\n")))
      println("")

      assert(splitter.getAllTerms == terms)
    }
  }

  it("should calculate the correct degree in q-thesis-deg-ex-01") {
    val treeDecomposition = TreeDecomposition.
      getTreeDecomposition(s"$pathToLine/gml/q-thesis-deg-ex-01.gml", s"$pathToLine/queries/q-thesis-deg-ex-01.cq")
    val splitter = Splitter(treeDecomposition._1)
    val flattenLogs = splitter.flattenLog().mkString("\n")
    println("Splitter ----------")
    println(println(flattenLogs))
    checkFlattenLogs(flattenLogs, s"$pathToExpectedSplitterLogs/q-thesis-deg-ex-01-logs.txt")

    assert(splitter.getAllTerms.size == 8)
  }


  it("should spit correctly q-thesis-deg-ex-02") {
    val treeDecomposition = TreeDecomposition.
      getTreeDecomposition(s"$pathToLine/gml/q-thesis-deg-ex-02.gml", s"$pathToLine/queries/q-thesis-deg-ex-02.cq")

    val centroid = treeDecomposition._1.getCentroid
    val branches = treeDecomposition._1.split(centroid)

    assert(branches.size == 2)

    val leftRoot = branches.head
    // should not revert
    assert(leftRoot.getRoot.atoms.map(_.getPredicate.getIdentifier.toString).contains("p21"))

    val leftCentroid =   leftRoot.getCentroid

    //should revert
    val leftBranches =  leftRoot.split(leftCentroid)
    val revertedLeftBranches = Array ( ("p23", ""), ("p22", "p23"), ("p21", "p22") )
    val vertexToCheck = Array(leftBranches.head, leftBranches.head.getChildren.head, leftBranches.head.getChildren.head.getChildren.head)

    for( i <- vertexToCheck.indices) {
      assert( vertexToCheck(i).getRoot.atoms.map(_.getPredicate.getIdentifier.toString).contains(revertedLeftBranches(i)._1))
      if(revertedLeftBranches(i)._2.nonEmpty) {
        assert( vertexToCheck(i).getParent.get.getRoot.atoms.map(_.getPredicate.getIdentifier.toString).contains(revertedLeftBranches(i)._2))
      }else{
        vertexToCheck(1).getParent.isEmpty
      }
    }

    assert(leftBranches.size == 2)

  }


  it("should calculate the correct degree in q-thesis-deg-ex-02") {
    val treeDecomposition = TreeDecomposition.
      getTreeDecomposition(s"$pathToLine/gml/q-thesis-deg-ex-02.gml", s"$pathToLine/queries/q-thesis-deg-ex-02.cq")
    val splitter = Splitter(treeDecomposition._1)

    val flattenLogs = splitter.flattenLog().mkString("\n")
    checkFlattenLogs(flattenLogs, s"$pathToExpectedSplitterLogs/q-thesis-deg-ex-02-logs.txt")
    
    println("Splitter ----------")
    println(println(flattenLogs))

    assert(splitter.getAllTerms.size == 16)
  }


  it("should calculate the correct degree in q-thesis-deg-ex-03") {
    val treeDecomposition = TreeDecomposition.
      getTreeDecomposition(s"$pathToLine/gml/q-thesis-deg-ex-03.gml", s"$pathToLine/queries/q-thesis-deg-ex-03.cq")

    val splitter = Splitter(treeDecomposition._1)

    val flattenLogs = splitter.flattenLog().mkString("\n")
    checkFlattenLogs(flattenLogs, s"$pathToExpectedSplitterLogs/q-thesis-deg-ex-03-logs.txt")

    println("Splitter ----------")
    println(println(flattenLogs))

    assert(splitter.getAllTerms.size == 36)
  }

  def checkFlattenLogs(flattenLogs:String, expectedLogfilePah:String ) = {
    val expected = getLogFile(expectedLogfilePah)
    for (i <- expected.indices) {
      assert(flattenLogs.contains(expected(i)), s"Expected: ${expected(i)}")
    }
  }

  private def getLogFile(logfilepath:String) : List[String] = {
    File(logfilepath).lines().toList
  }

  private def getTerms(terms: List[String]): Set[Term] = {
    terms.map(term => {
      DefaultTermFactory.instance.createVariable(term)
    }).toSet
  }

  private def getAtom(identifier: String, terms: Set[Term]): Atom = {
    val sPredicate: Predicate = new Predicate(identifier, 2)
    new DefaultAtom(sPredicate, terms.toList.asJava)
  }
}
