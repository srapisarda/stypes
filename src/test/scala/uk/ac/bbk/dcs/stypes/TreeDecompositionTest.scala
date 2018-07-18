package uk.ac.bbk.dcs.stypes

/*
 * #%L
 * stypes
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

import org.junit.Assert
import org.scalatest.FunSpec

/**
  * Created by Salvatore Rapisarda on 27/04/2017.
  */
class TreeDecompositionTest extends FunSpec {

  describe("Tree decomposition commons ") {

    it("should a common operation correctly 2 ") {
      val t = buildTestTreeDecomposition("src/test/resources/q11.gml", "src/test/resources/q11.txt")
      assert(t.getRoot.atoms.size==1)


    }

    it("should make common operation correctly") {

      val t = buildTestHyperTreeDecomposition("src/test/resources/Q7.gml", "src/test/resources/Q7.cq")

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

      val terms = t.getAllTerms
      assert(terms.size == 8)

    }

  }

  private def buildTestHyperTreeDecomposition(fileGML: String, fileCQ: String): TreeDecomposition =
    TreeDecomposition.getHyperTreeDecomposition(fileGML, fileCQ)

  private def buildTestTreeDecomposition(fileGML: String, fileCQWithHead: String): TreeDecomposition = {
    TreeDecomposition.getTreeDecomposition(fileGML, fileCQWithHead)._1
  }


}
