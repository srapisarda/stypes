package uk.ac.bbk.dcs.stypes

/*
 * #%L
 * STypeS
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
      assert(t.getRoot.atoms.size == 1)
    }

    it("should make common operation correctly") {

      val t = buildTestHyperTreeDecomposition("src/test/resources/Q7.gml", "src/test/resources/Q7.cq")

      Assert.assertNotNull(t)
      assert(Set("X2", "X3") == t.getRoot.variables.map(_.getIdentifier))
      //      System.out.println("root: " + t.getRoot.variables)
      Assert.assertEquals(2, t.getChildren.size)
      Assert.assertEquals(7, t.getSize)
      //      // assert splitter atoms's terms
      val separator: TreeDecomposition = t.getCentroid
      separator.getRoot.variables.foreach(term =>
        assert(Set("X3", "X4").contains(term.getIdentifier.asInstanceOf[String])))

      //
      val s = Splitter(t)

      println(s)

      val terms = t.getAllTerms
      assert(terms.size == 8)

    }


    it("should contains vertex p3") {
      val t = buildTestTreeDecomposition("src/test/resources/benchmark/Lines/gml/q-thesis-deg-ex-01.gml", "src/test/resources/benchmark/Lines/queries/q-thesis-deg-ex-01.cq")

      Assert.assertNotNull(t)

      val centroid = t.getCentroid
      Assert.assertTrue(
        t.contains(centroid))

     val children = t.split(centroid)

      assert( children.size == 2)

    }

    it("should correctly split using centroid p3") {
      val t = buildTestTreeDecomposition("src/test/resources/benchmark/Lines/gml/q-thesis-deg-ex-02.gml", "src/test/resources/benchmark/Lines/queries/q-thesis-deg-ex-02.cq")
      val centroid = t.getCentroid
      val children = t.split(centroid)

      assert(children.size == 2)
      assert(children.map(_.getParent.isEmpty).forall(b => b))

      var checked = 0;
      children.foreach(c1 => {
        val c2 = t.getChildren.find(c2=> c2.getRoot == c1.getRoot ).get

        assert(c1.getChildren.map(chil =>
          chil.getParent.get == c1)
          .forall(b => b))

        assert(c2.getChildren == c1.getChildren)

        checked += 1
      })
      assert(checked == 2 )
    }



    it("should contains vertex p1, p2") {
      val t = buildTestTreeDecomposition("src/test/resources/benchmark/Lines/gml/q-thesis-ex-01.gml", "src/test/resources/benchmark/Lines/queries/q-thesis-ex-01.cq")

      Assert.assertNotNull(t)

    }

    it("should contains vertex centroid of S3 child") {
      val t = buildTestTreeDecomposition("src/test/resources/benchmark/Lines/gml/q-thesis-deg-ex-01.gml", "src/test/resources/benchmark/Lines/queries/q-thesis-deg-ex-01.cq")

      Assert.assertNotNull(t)

      val centroid = t.getChildren.head.getCentroid
      Assert.assertTrue(
        t.contains(centroid))
    }

    it("should not contains its parent as children") {
      val t = buildTestTreeDecomposition("src/test/resources/benchmark/Lines/gml/q-thesis-deg-ex-01.gml", "src/test/resources/benchmark/Lines/queries/q-thesis-deg-ex-01.cq")

      Assert.assertNotNull(t)

      val centroid = t.getChildren.head.getCentroid
      Assert.assertFalse(
        centroid.contains(t))
    }

    it("should get the path to centroid") {
      val t = buildTestTreeDecomposition("src/test/resources/benchmark/Lines/gml/q-thesis-deg-ex-01.gml", "src/test/resources/benchmark/Lines/queries/q-thesis-deg-ex-01.cq")

      Assert.assertNotNull(t)

      val centroid = t.getCentroid
      val path = t.getPathTo(centroid)

      Assert.assertEquals(1, path.size)

    }

    it("should get the path to child of child") {
      val t = buildTestTreeDecomposition("src/test/resources/benchmark/Lines/gml/q-thesis-deg-ex-01.gml", "src/test/resources/benchmark/Lines/queries/q-thesis-deg-ex-01.cq")

      Assert.assertNotNull(t)

      val v = t.getChildren.head.getChildren.head

      val path = t.getPathTo(v)

      Assert.assertEquals(3, path.size)
      Assert.assertEquals(t, path.head)
      Assert.assertEquals(t.getChildren.head, path.toVector(1))
      Assert.assertEquals(t.getChildren.head.getChildren.head, path.toVector(2))
    }

    it("should get the path to child from child of child") {
      val t = buildTestTreeDecomposition("src/test/resources/benchmark/Lines/gml/q-thesis-deg-ex-01.gml", "src/test/resources/benchmark/Lines/queries/q-thesis-deg-ex-01.cq")

      Assert.assertNotNull(t)

      val v = t.getChildren.head.getChildren.head

      val path = TreeDecomposition.getPathTo(v, t.getChildren.head)

      Assert.assertEquals(2, path.size)
      Assert.assertEquals(t.getChildren.head, path.toVector(0))
      Assert.assertEquals(t.getChildren.head.getChildren.head, path.toVector(1))
    }

    it("should get first common vertex between two paths") {
      val t = buildTestTreeDecomposition("src/test/resources/benchmark/Lines/gml/q-thesis-deg-ex-02.gml", "src/test/resources/benchmark/Lines/queries/q-thesis-deg-ex-02.cq")

      Assert.assertNotNull(t)

      val v = t.getChildren.head.getChildren.head

      val one =  t
      val two = t.getChildren.head
      val three1 = t.getChildren.head.getChildren.head
      val three2 = t.getChildren.tail.head.getChildren.head
      val four1 = t.getChildren.head.getChildren.head.getChildren.head
      val four2 = t.getChildren.tail.head.getChildren.head.getChildren.head

      val path1 = one:: two :: three1 :: four1 :: Nil
      val path2 = one:: two :: three2 :: four2 :: Nil

      val commonVertex = TreeDecomposition.getLastCommonVertex(path1, path2)

      assert(commonVertex==two)
    }


  }

  private def buildTestHyperTreeDecomposition(fileGML: String, fileCQ: String): TreeDecomposition =
    TreeDecomposition.getHyperTreeDecomposition(fileGML, fileCQ)

  private def buildTestTreeDecomposition(fileGML: String, fileCQWithHead: String): TreeDecomposition = {
    TreeDecomposition.getTreeDecomposition(fileGML, fileCQWithHead)._1
  }


}
