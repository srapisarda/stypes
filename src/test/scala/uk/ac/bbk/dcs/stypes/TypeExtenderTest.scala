package uk.ac.bbk.dcs.stypes

import java.io.File
import java.util

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Rule, Term}
import fr.lirmm.graphik.graal.api.factory.TermFactory
import fr.lirmm.graphik.graal.core.{DefaultAtom, TreeMapSubstitution}
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


  val tf: TermFactory = DefaultTermFactory.instance
  val rterms: util.List[Term] = new util.ArrayList[Term]
  rterms.add(tf.createVariable("X2"))
  rterms.add(tf.createVariable("Y2"))
  val rPredicate: Predicate = new Predicate("r1", 2)
  val r: Atom = new DefaultAtom(rPredicate, rterms)


  def getMockTypeMixed: Type = {
    val s1 = new TreeMapSubstitution
    val tx = DefaultTermFactory.instance.createVariable("X2")
    val ee0  =   new ConstantType(0, "EE0") // DefaultTermFactory.instance.createConstant( )
    s1.put(tx, ee0)
    val ty = DefaultTermFactory.instance.createVariable( "X3")
    s1.put(ty, ConstantType.EPSILON)

    val atom = new DefaultAtom(new Predicate("u1", 2))
    atom.setTerm(0, DefaultTermFactory.instance.createConstant("a0"))
    atom.setTerm(1, DefaultTermFactory.instance.createConstant("a1"))

    Type(Map(tx-> atom, ty-> atom), s1 )
  }

  var atoms= List(r)

  describe("TypeExtender  decomposition commons ") {

    it("should  create the children property") {
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition= test.buildTestTreeDecomposition
      val s = getMockTypeMixed
      val reWriter  = new ReWriter(ontology)
      val canonicalModels =  reWriter.canonicalModels
      val te: TypeExtender = new TypeExtender(t.getRoot,  s.homomorphism , canonicalModels.toArray, atoms  )

      assert(te!=null)
      assert(te.children.nonEmpty)
    }


  }

}
