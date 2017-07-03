package uk.ac.bbk.dcs.stypes

import java.io.File

import fr.lirmm.graphik.graal.api.core.Rule
import fr.lirmm.graphik.graal.core.TreeMapSubstitution
import fr.lirmm.graphik.graal.core.ruleset.LinkedListRuleSet
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import fr.lirmm.graphik.graal.io.dlp.{DlgpParser, DlgpWriter}
import org.scalatest.FunSpec

/**
  * Created by rapissal on 26/06/2017.
  */
class TypeExtenderTest extends FunSpec {

  // 0 - Create a Dlgp writer and a structure to store rules.
  private val writer = new DlgpWriter
  private val ontology = new LinkedListRuleSet

  // 2 - Parse Animals.dlp (A Dlgp file with rules and facts)
  val dlgpParser = new DlgpParser(new File("src/main/resources/ont-1.dlp" ))
  while (dlgpParser.hasNext) {
    dlgpParser.next match {
      case rule: Rule => ontology.add(rule)
      case  _ =>  // println("do nothing")
    }
  }

  def getMockEpsilonType: Type = {
    val s1 = new TreeMapSubstitution
    val tx = DefaultTermFactory.instance.createVariable("X2")
    s1.put(tx, ConstantType.EPSILON)
    Type(null, s1 )
  }

  def getMockAnonymousType: Type = {
    val s1 = new TreeMapSubstitution
    val tx = DefaultTermFactory.instance.createVariable("X2")
    val ee0  =   new ConstantType(0, "EE0") // DefaultTermFactory.instance.createConstant( )
    s1.put(tx, ee0)

    Type(null, s1 )
  }


  def testExtension( s: Type): TypeExtender ={
    val test:TreeDecompositionTest  = new TreeDecompositionTest
    val t:TreeDecomposition= test.buildTestTreeDecomposition
    val reWriter  = new ReWriter(ontology)
    val canonicalModels =  reWriter.canonicalModels
    new TypeExtender(t.getRoot,  s.homomorphism , canonicalModels.toArray, t.getRoot.atoms.toList )
  }

  describe("TypeExtender  decomposition commons ") {

    it("should  create the children property on anonymous individual ") {
      val te = testExtension(getMockAnonymousType)
      assert(te!=null)
      assert(te.children.nonEmpty)
      assert(te.children.head.children.isEmpty)
    }

    it("should  create the children property on EPSILON individual ") {
      val te = testExtension(getMockEpsilonType)
      assert(te!=null)
      assert(te.children.nonEmpty)
      assert(te.children.head.children.isEmpty)
    }


  }

}
