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

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import fr.lirmm.graphik.graal.core.{DefaultAtom, TreeMapSubstitution}
import fr.lirmm.graphik.graal.io.dlp.DlgpWriter
import org.scalatest.FunSpec

/**
  * Created by rapissal on 26/06/2017.
  */
class TypeExtenderTest extends FunSpec {

  describe("TypeExtender  decomposition commons ") {

    it("should  create the children properly on anonymous individual ") {
      val te = testExtension(getMockAnonymousType, None)
      assert(te!=null)
      assert(te.children.nonEmpty)
      assert(te.children.head.children.isEmpty)
      assert(te.types.lengthCompare(1) == 0)
    }

    it("should  create the children properly on EPSILON individual ") {
      val te = testExtension(getMockEpsilonType, None)
      assert(te!=null)
      assert(te.children.lengthCompare(3) == 0)
      assert(te.children.head.children.isEmpty)
      assert(te.types.lengthCompare(3) == 0)
    }


    it ( "should filter the atoms and return true ") {
      val ret = getMockFilterAtoms(true)
      val te = testExtension(ret._1, Some(ret._2))
      assert(te.isValid)
      assert(te.types.lengthCompare(1) == 0)
    }

    it ( "should filter the atoms and return false ") {
      val ret = getMockFilterAtoms(false)
      val te = testExtension(ret._1, Some(ret._2))
      assert(!te.children.head.isValid)
      assert(te.types.isEmpty)
    }

    it("should  collect the leave types from the extender ") {
      val extender = testExtension(getEmptyType, None)
      val types = extender.collectTypes
      assert(types.lengthCompare(5) == 0)
    }
  }


  // 0 - Create a Dlgp writer and a structure to store rules.
  private val writer = new DlgpWriter
  private val ontology = ReWriter.getOntology("src/main/resources/ont-1.dlp")

  def getMockEpsilonType: Type = {
    val s1 = new TreeMapSubstitution
    val tx = DefaultTermFactory.instance.createVariable("X2")
    s1.put(tx, ConstantType.EPSILON)
    Type( s1 )
  }

  def getEmptyType: Type = {
    val s1 = new TreeMapSubstitution
    Type( s1 )
  }

  def getMockAnonymousType: Type = {
    val s1 = new TreeMapSubstitution
    val tx = DefaultTermFactory.instance.createVariable("X2")
    val ee0  =   new ConstantType(0, "EE0") // DefaultTermFactory.instance.createConstant( )
    s1.put(tx, ee0)

    Type( s1 )
  }


  def getMockFilterAtoms(switch:Boolean): (Type, Bag)  ={
    val tx1 = DefaultTermFactory.instance.createVariable("X1")
    val tx2 = DefaultTermFactory.instance.createVariable("X2")

    val atom1 = new DefaultAtom(new Predicate("r1", 2))
    atom1.setTerm(0, tx1)
    atom1.setTerm(1, tx2)

    val atom2 = new DefaultAtom(new Predicate( if (switch) "r1" else "r2", 2))
    atom2.setTerm(0, tx2)
    atom2.setTerm(1, tx1)


    val atoms:Set[Atom] = Set(atom1, atom2)
    val terms:Set[Term] = Set(tx1, tx2)
    val bag:Bag = Bag( atoms, terms)

    val t:Type = getMockAnonymousType

    (t, bag)

  }

  def testExtension( s: Type, opBag: Option[Bag] ): TypeExtender ={
    val t:TreeDecomposition= TreeDecomposition.getHyperTreeDecomposition("src/main/resources/Q7.gml", "src/main/resources/Q7.cq")
    val reWriter  = new ReWriter(ontology)
    val canonicalModels =  reWriter.canonicalModels

    new TypeExtender(  if(opBag.isDefined) opBag.get else t.getRoot,  s.homomorphism , canonicalModels.toVector )
  }



}
