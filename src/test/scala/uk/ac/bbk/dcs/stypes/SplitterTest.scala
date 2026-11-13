package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.scalatest.funspec.AnyFunSpec

import scala.jdk.CollectionConverters._

class SplitterTest extends AnyFunSpec {

  describe("Splitter") {

    it("should construct linear splitters from a path decomposition in leaf mode") {
      val treeDecomposition =
        TreeDecomposition.getTreeDecomposition("src/test/resources/q11.gml", "src/test/resources/q11.txt")._1
      val pathDecomposition = treeDecomposition.asPathDecomposition

      def nodes(tree: TreeDecomposition): List[TreeDecomposition] =
        tree :: tree.getChildren.flatMap(nodes)

      def atoms(tree: TreeDecomposition): Set[Atom] =
        tree.getRoot.atoms ++ tree.getChildren.flatMap(atoms)

      def leafCount(tree: TreeDecomposition): Int =
        nodes(tree).count { node =>
          node.getChildren.size + (if (node eq tree) 0 else 1) <= 1
        }

      def maxChildren(splitter: Splitter): Int =
        splitter.children.map(maxChildren).foldLeft(splitter.children.size)(math.max)

      val originalNodes = nodes(treeDecomposition)
      val pathNodes = nodes(pathDecomposition)
      val originalWidth = originalNodes.map(_.getRoot.variables.size).max
      val numberOfLeaves = leafCount(treeDecomposition)

      assert(pathDecomposition.getSize == treeDecomposition.getSize)
      assert(pathDecomposition.getChildren.size <= 1)
      assert(atoms(pathDecomposition) == atoms(treeDecomposition))
      assert(pathNodes.forall(node => node.getRoot.atoms.forall { atom =>
        atom.getTerms.asScala.forall(node.getRoot.variables.contains)
      }))
      assert(pathNodes.map(_.getRoot.variables.size).max <= originalWidth * numberOfLeaves)
      treeDecomposition.getAllTerms.foreach { variable =>
        val positions = pathNodes.zipWithIndex.collect {
          case (node, index) if node.getRoot.variables.contains(variable) => index
        }
        assert(positions == (positions.head to positions.last))
      }

      val splitter = Splitter(treeDecomposition, Leaf)
      assert(maxChildren(splitter) <= 1)
      assert(splitter.getAllTerms == treeDecomposition.getAllTerms)
    }

    it("should return all terms") {
      val terms = getTerms(List("x6", "x7", "x2", "x3", "x0", "x4", "x1", "x5"))
      val pathToLine = "src/test/resources/benchmark/Lines"
      val treeDecomposition =  TreeDecomposition.
        getTreeDecomposition(s"$pathToLine/gml/q-thesis-1.multivar.gml", s"$pathToLine/queries/q-thesis-1.cq")
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
