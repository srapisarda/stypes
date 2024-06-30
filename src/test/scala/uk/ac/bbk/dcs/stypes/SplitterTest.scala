package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.scalatest.FunSpec

import scala.collection.JavaConverters._

class SplitterTest extends FunSpec {

  private val pathToLine = "src/test/resources/benchmark/Lines"

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


    assert(flattenLogs.contains("splitterBag: Set(p1[2](x0,x1)), children: 2"))
    assert(flattenLogs.contains("splitterBag: Set(p3[2](x2,x3)), children: 2, parent: Set(p1[2](x0,x1)"))
    assert(flattenLogs.contains("splitterBag: Set(p2[2](x1,x2)), children: 0, parent: Set(p3[2](x2,x3)"))
    assert(flattenLogs.contains("splitterBag: Set(p4[2](x3,x4)), children: 0, parent: Set(p3[2](x2,x3)"))
    assert(flattenLogs.contains("splitterBag: Set(p6[2](x5,x6)), children: 2, parent: Set(p1[2](x0,x1)"))
    assert(flattenLogs.contains("splitterBag: Set(p5[2](x0,x5)), children: 0, parent: Set(p6[2](x5,x6)"))
    assert(flattenLogs.contains("splitterBag: Set(p7[2](x6,x7)), children: 0, parent: Set(p6[2](x5,x6)"))


    assert(splitter.getAllTerms.size == 8)
  }


  it("should spit correctly q-thesis-deg-ex-02") {
    val treeDecomposition = TreeDecomposition.
      getTreeDecomposition(s"$pathToLine/gml/q-thesis-deg-ex-02.gml", s"$pathToLine/queries/q-thesis-deg-ex-02.cq")

    val centroid = treeDecomposition._1.getCentroid
    val branches = treeDecomposition._1.split(centroid)

    assert(branches.size == 2)
  }


    it("should calculate the correct degree in q-thesis-deg-ex-02") {
    val treeDecomposition = TreeDecomposition.
      getTreeDecomposition(s"$pathToLine/gml/q-thesis-deg-ex-02.gml", s"$pathToLine/queries/q-thesis-deg-ex-02.cq")
    val splitter = Splitter(treeDecomposition._1)

    val flattenLogs = splitter.flattenLog().mkString("\n")

    assert(flattenLogs.contains("splitterBag: Set(p1[2](x0,x1)), children: 2, parent:"))
    assert(flattenLogs.contains("splitterBag: Set(p3[2](x4,x5)), children: 2, parent: Set(p1[2](x0,x1))"))
    assert(flattenLogs.contains("splitterBag: Set(p22[2](x2,x3)), children: 2, parent: Set(p3[2](x4,x5))"))
    assert(flattenLogs.contains("splitterBag: Set(p21[2](x1,x2)), children: 0, parent: Set(p22[2](x2,x3))"))
    assert(flattenLogs.contains("splitterBag: Set(p23[2](x3,x4)), children: 0, parent: Set(p22[2](x2,x3))"))
    assert(flattenLogs.contains("splitterBag: Set(p42[2](x6,x7)), children: 2, parent: Set(p3[2](x4,x5))"))
    assert(flattenLogs.contains("splitterBag: Set(p41[2](x5,x6)), children: 0, parent: Set(p42[2](x6,x7))"))
    assert(flattenLogs.contains("splitterBag: Set(p43[2](x7,x8)), children: 0, parent: Set(p42[2](x6,x7))"))
    assert(flattenLogs.contains("splitterBag: Set(p6[2](x11,x12)), children: 2, parent: Set(p1[2](x0,x1))"))
    assert(flattenLogs.contains("splitterBag: Set(p52[2](x9,x10)), children: 2, parent: Set(p6[2](x11,x12))"))
    assert(flattenLogs.contains("splitterBag: Set(p51[2](x0,x9)), children: 0, parent: Set(p52[2](x9,x10))"))
    assert(flattenLogs.contains("splitterBag: Set(p53[2](x10,x11)), children: 0, parent: Set(p52[2](x9,x10))"))
    assert(flattenLogs.contains("splitterBag: Set(p72[2](x13,x14)), children: 2, parent: Set(p6[2](x11,x12))"))
    assert(flattenLogs.contains("splitterBag: Set(p71[2](x12,x13)), children: 0, parent: Set(p72[2](x13,x14))"))
    assert(flattenLogs.contains("splitterBag: Set(p73[2](x14,x15)), children: 0, parent: Set(p72[2](x13,x14))"))
    
    println("Splitter ----------")
    println(println(flattenLogs))

    assert(splitter.getAllTerms.size == 16)
  }


  it("should calculate the correct degree in q-thesis-deg-ex-03") {
    val treeDecomposition = TreeDecomposition.
      getTreeDecomposition(s"$pathToLine/gml/q-thesis-deg-ex-03.gml", s"$pathToLine/queries/q-thesis-deg-ex-03.cq")

    val splitter = Splitter(treeDecomposition._1)

    val flattenLogs = splitter.flattenLog().mkString("\n")


    println("Splitter ----------")
    println(println(flattenLogs))

    assert(splitter.getAllTerms.size == 36)
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
