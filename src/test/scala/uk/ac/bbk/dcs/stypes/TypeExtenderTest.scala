package uk.ac.bbk.dcs.stypes

import java.io.File

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Rule, Term}
import fr.lirmm.graphik.graal.core.{DefaultAtom, TreeMapSubstitution}
import fr.lirmm.graphik.graal.core.ruleset.LinkedListRuleSet
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import fr.lirmm.graphik.graal.io.dlp.{DlgpParser, DlgpWriter}
import org.scalatest.FunSpec

/**
  * Created by rapissal on 26/06/2017.
  */
class TypeExtenderTest extends FunSpec {

  describe("TypeExtender  decomposition commons ") {

    it("should  create the children property on anonymous individual ") {
      val te = testExtension(getMockAnonymousType, None)
      assert(te!=null)
      assert(te.children.nonEmpty)
      assert(te.children.head.children.isEmpty)
    }

    it("should  create the children property on EPSILON individual ") {
      val te = testExtension(getMockEpsilonType, None)
      assert(te!=null)
      assert(te.children.length == 3)
      assert(te.children.head.children.isEmpty)
    }


    it ( "should filter the atoms and return true ") {
      val ret = getMockFileterAtoms(true)
      val te = testExtension(ret._1, Some(ret._2))
      assert(te.isValid)
    }

    it ( "should filter the atoms and return false ") {
      val ret = getMockFileterAtoms(false)
      val te = testExtension(ret._1, Some(ret._2))
      assert(!te.children.head.isValid)
    }

  }


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


  def getMockFileterAtoms( switch:Boolean): (Type, Bag)  ={
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
    val test:TreeDecompositionTest  = new TreeDecompositionTest
    val t:TreeDecomposition= test.buildTestTreeDecomposition
    val reWriter  = new ReWriter(ontology)
    val canonicalModels =  reWriter.canonicalModels

    new TypeExtender(  if(opBag.isDefined) opBag.get else t.getRoot,  s.homomorphism , canonicalModels.toArray )
  }



}
