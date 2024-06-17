package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.scalatest.FunSpec

import java.util
import scala.collection.JavaConverters._

class SplitterTest extends FunSpec {

  describe("Splitter") {
//    it("should return the splitting vertex") {
//      val splitter = Splitter(TreeDecomposition(Bag(Set(Atom("a"), Atom("b")), List.empty)))
//      assert(splitter.getSplittingVertex == Bag(Set(Atom("a"), Atom("b")), List.empty))
//    }

    it("should return all terms") {
      val terms = getTerms(List("x6", "x7", "x2", "x3", "x0", "x4", "x1", "x5"))
      val pathToLine = "src/test/resources/benchmark/Lines"
      val treeDecomposition =  TreeDecomposition.
        getTreeDecomposition(s"$pathToLine/gml/q-thesis-1.bkp.gml", s"$pathToLine/queries/q-thesis-1.cq")
      val splitter = Splitter(treeDecomposition._1)


      println("Splitter ----------")
      println(println(splitter.flattenLog().mkString("\n")))
      println("")

      assert(splitter.getAllTerms == terms)
    }
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
