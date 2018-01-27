package uk.ac.bbk.dcs.stypes

/*
 * #%L
 * stype
 * %%
 * Copyright (C) 2017 - 2021 Birkbeck University of London
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.tinkerpop.blueprints.Graph
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.blueprints.util.io.gml.GMLReader
import fr.lirmm.graphik.graal.api.core.Predicate
import fr.lirmm.graphik.graal.api.factory.TermFactory
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.junit.Assert
import org.scalatest.FunSpec

import scala.io.Source
import scala.reflect.io.File
import scala.util.matching.Regex

/**
  * Created by Salvatore Rapisarda on 27/04/2017.
  */
class TreeDecompositionTest extends FunSpec {

  def buildTestTreeDecomposition(fileGML: String, fileCQ: String): TreeDecomposition = {

    val pattern: Regex = "(?<=\\()[^)]+(?=\\))".r
    val tf: TermFactory = DefaultTermFactory.instance
    val lines = Source.fromFile(fileCQ).getLines().toList

    val atoms = lines.map(
      line => {
        val termss = pattern
          .findAllIn(line)
          .flatMap(p => p.split(","))
          .toList.map(_.trim)
        val terms = termss.map(tf.createVariable(_))

        val predicateName = line.split('(').head
        val predicate: Predicate = new Predicate(predicateName, terms.length)

        new DefaultAtom(predicate, terms: _*)
      }
    )

    val graph: Graph = new TinkerGraph
    val in = File(fileGML).inputStream()

    GMLReader.inputGraph(graph, in)
    new TreeDecomposition(atoms.toSet, graph, null)

  }

  describe("Tree decomposition commons ") {
    it("should make common operation correctly") {

      val t = buildTestTreeDecomposition("src/main/resources/Q7.gml", "src/main/resources/Q7.cq")

      Assert.assertNotNull(t)
      assert(Set("X2", "X3") == t.getRoot.variables.map(_.getIdentifier))
      //      System.out.println("root: " + t.getRoot.variables)
      Assert.assertEquals(2, t.getChildes.size)
      Assert.assertEquals(7, t.getSize)
      //      // assert splitter atoms's terms
      val separator: TreeDecomposition = t.getSeparator
      separator.getRoot.variables.foreach(term =>
        assert(Set("X3", "X4").contains(term.getIdentifier.asInstanceOf[String])))

      //
      val s = Splitter(t)

      println(s)

      val terms = t.getAllTerms()
      assert(terms.size == 8)

    }

  }

}
