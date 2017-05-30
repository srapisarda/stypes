package uk.ac.bbk.dcs.stypes

import java.io.File

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Rule}
import fr.lirmm.graphik.graal.core.{DefaultAtom, TreeMapSubstitution}
import fr.lirmm.graphik.graal.core.ruleset.LinkedListRuleSet
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import fr.lirmm.graphik.graal.io.dlp.{DlgpParser, DlgpWriter}
import fr.lirmm.graphik.graal.store.rdbms.DefaultRdbmsStore
import fr.lirmm.graphik.graal.store.rdbms.driver.HSQLDBDriver
import org.scalatest.FunSpec

import scala.collection.JavaConverters._

/**
  * Created by
  *   Salvatore Rapisarda
  *   Stanislav Kikot
  *
  *
  *   on 09/05/2017.
  */
class ReWriterTest extends FunSpec{

  // 0 - Create a Dlgp writer and a structure to store rules.
  val writer = new DlgpWriter
  val ontology = new LinkedListRuleSet

  // 1 - Create a relational database store with HSQLDB (An InMemory Java
  // database system),
  val store = new DefaultRdbmsStore(new HSQLDBDriver("test", null))

  // 2 - Parse Animals.dlp (A Dlgp file with rules and facts)
  val dlgpParser = new DlgpParser(new File("src/main/resources/ont-1.dlp" ))
  while (dlgpParser.hasNext) {
     dlgpParser.next match {
      case atom: Atom => store.add(atom)
      case rule: Rule => ontology.add(rule)
      case  _ =>  // println("do nothing")
    }
  }



  def getMockTypeEpsilon: Type = {
    val s1 = new TreeMapSubstitution
    val tx = DefaultTermFactory.instance.createVariable("X2")
    val epsilon  =  ConstantType.EPSILON
    s1.put(tx, epsilon)
    val ty = DefaultTermFactory.instance.createVariable( "X3")
    s1.put(ty, epsilon)
    Type(null, s1 )
  }


  def getMockTypeAnonymous: Type = {
    val s1 = new TreeMapSubstitution
    val tx = DefaultTermFactory.instance.createVariable("X2")
    val ee0  =  DefaultTermFactory.instance.createConstant("EE0")
    s1.put(tx, ee0)
    val ty = DefaultTermFactory.instance.createVariable( "X3")
    s1.put(ty, ee0)

    val atom = new DefaultAtom(new Predicate("u1", 2))
    atom.setTerm(0, DefaultTermFactory.instance.createConstant("a0"))
    atom.setTerm(1, DefaultTermFactory.instance.createConstant("a1"))

    Type(Map(tx-> atom, ty-> atom), s1 )
  }

  def getMockTypeMixed: Type = {
    val s1 = new TreeMapSubstitution
    val tx = DefaultTermFactory.instance.createVariable("X2")
    val ee0  =  DefaultTermFactory.instance.createConstant("EE0")
    s1.put(tx, ee0)
    val ty = DefaultTermFactory.instance.createVariable( "X3")
    s1.put(ty, ConstantType.EPSILON)

    val atom = new DefaultAtom(new Predicate("u1", 2))
    atom.setTerm(0, DefaultTermFactory.instance.createConstant("a0"))
    atom.setTerm(1, DefaultTermFactory.instance.createConstant("a1"))

    Type(Map(tx-> atom, ty-> atom), s1 )
  }



  describe("ReWriter test cases") {

    it("should contains at least 2 atoms") {
       val atoms =  ReWriter.makeGeneratingAtoms( ontology)
        assert(atoms.size == 2)
//       println (atoms)

    }

    it("should create the canonical models from 2 atoms") {
        val canonicalModels = ReWriter.canonicalModelList(ontology)
        assert(canonicalModels.length == 2)
        canonicalModels.foreach( atomSet => assert(atomSet.asScala.size == 4))
//        println(canonicalModels)
    }


    it("should get the epsilon "){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition= test.buildTestTreeDecomposition
      val s = getMockTypeEpsilon
      val atoms = new ReWriter(ontology).makeAtoms( t.getRoot,s)
      println(atoms)
      assert( atoms.length==1 )
    }

    it("should get the anonymous "){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition= test.buildTestTreeDecomposition
      val s = getMockTypeAnonymous
      val atoms = new ReWriter(ontology).makeAtoms( t.getRoot,s)
      println(atoms)
      assert( atoms.length==1 )

    }

    it("should get the empty set for mixed type   "){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition= test.buildTestTreeDecomposition
      val s = getMockTypeMixed
     // assertThrows[Exception] { new ReWriter(ontology).makeAtoms( t.getRoot,s) }
      //println(atoms)
     // assert( atoms.length==0 )
    }




  }



}
