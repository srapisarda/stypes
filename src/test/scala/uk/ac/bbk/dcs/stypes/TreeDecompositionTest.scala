package uk.ac.bbk.dcs.stypes


import com.tinkerpop.blueprints.Graph
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.blueprints.util.io.gml.GMLReader
import fr.lirmm.graphik.graal.api.core.{Atom, Predicate}
import fr.lirmm.graphik.graal.api.factory.TermFactory
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.junit.Assert
import org.scalatest.FunSpec

import scala.reflect.io.File


/**
  * Created by Salvatore Rapisarda on 27/04/2017.
  */
class TreeDecompositionTest extends FunSpec {

  def buildTestTreeDecomposition: TreeDecomposition= {
    val pt = Map(
      "r1" -> List("X2", "X3"),
      "s1" -> List("X3", "X4"),
      "s2" -> List("X4", "X5"),
      "r2" -> List("X5", "X6"),
      "s3" -> List("X6", "X7"),
      "r0" -> List("X1", "X2"),
      "s0" -> List("X0", "X1")
    )


    val tf: TermFactory = DefaultTermFactory.instance
    val atoms: Set[Atom] = pt.map(entry => {
      val predicate: Predicate = new Predicate(entry._1, entry._2.size)
      val terms = entry._2.map(tf.createVariable(_)) // getValue.stream.map(tf.createVariable).collect(Collectors.toList)
      new DefaultAtom(predicate, terms: _*)

    }).toSet

    val graph: Graph = new TinkerGraph
    val in = File("src/main/resources/Q7.gml").inputStream()

    GMLReader.inputGraph(graph, in)
    new  TreeDecomposition(atoms, graph, null)


  }

  describe("Tree decomposition commons ") {
    it("should make common operation correctly") {

      val t = buildTestTreeDecomposition

      Assert.assertNotNull(t)
      assert(Set("X2", "X3") == t.getRoot.variables.map(_.getIdentifier))
//      System.out.println("root: " + t.getRoot.variables)
      Assert.assertEquals(2, t.getChildes.size)
      Assert.assertEquals(7, t.getSize)
      //      // assert splitter atoms's terms
      val separator: TreeDecomposition = t.getSplitter
      separator.getRoot.variables.foreach(term =>
        assert(Set("X3", "X4").contains(term.getIdentifier.asInstanceOf[String])))

      //
      val s =  new  Splitter(t)

      println(s)

    }

  }

}
