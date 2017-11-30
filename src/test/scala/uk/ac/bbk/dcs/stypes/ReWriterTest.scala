package uk.ac.bbk.dcs.stypes

import java.io.File

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Rule, Term}
import fr.lirmm.graphik.graal.core.ruleset.LinkedListRuleSet
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import fr.lirmm.graphik.graal.core.{DefaultAtom, TreeMapSubstitution}
import fr.lirmm.graphik.graal.io.dlp.{DlgpParser, DlgpWriter}
import fr.lirmm.graphik.util.DefaultURI
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
class  ReWriterTest extends FunSpec{

  // 0 - Create a Dlgp writer and a structure to store rules.
  val writer = new DlgpWriter
  private val ontology1 = getOntology("src/main/resources/ont-1.dlp")
  private val ontology2 = getOntology("src/main/resources/ont-2.dlp")
  private val ontology3 = getOntology("src/main/resources/ont-3.dlp")
  private val ontology4 = getOntology("src/main/resources/ont-4.dlp")
  private val ontology5 = getOntology("src/main/resources/ont-5.dlp")

  // 1 - Create a relational database store with HSQLDB (An InMemory Java
  // database system),
  //  val store = new DefaultRdbmsStore(new HSQLDBDriver("test", null))

  private def getOntology (filename:String ) = {
    // 2 - Parse Animals.dlp (A Dlgp file with rules and facts)
    val dlgpParser = new DlgpParser(new File(filename))
    // val store = new DefaultRdbmsStore(new HSQLDBDriver("test", null))
    val ontology = new LinkedListRuleSet
    while (dlgpParser.hasNext) {
      dlgpParser.next match {
        // case atom: Atom => store.add(atom)
        case rule: Rule => ontology.add(rule)
        case _ => // println("do nothing")
      }
    }

    ontology
  }



  def getMockTypeEpsilon: Type = {
    val s1 = new TreeMapSubstitution
    val tx = DefaultTermFactory.instance.createVariable("X2")
    val epsilon  =  ConstantType.EPSILON
    s1.put(tx, epsilon)
    val ty = DefaultTermFactory.instance.createVariable( "X3")
    s1.put(ty, epsilon)
    Type(s1 )
  }


  def getMockTypeAnonymous: Type = {
    val s1 = new TreeMapSubstitution
    val tx = DefaultTermFactory.instance.createVariable("X2")
    val ee0  =  new ConstantType(0, "EE0") // DefaultTermFactory.instance.createConstant("EE0")
    s1.put(tx, ee0)
    val ty = DefaultTermFactory.instance.createVariable( "X3")
    s1.put(ty, ee0)

    val atom = new DefaultAtom(new Predicate("u1", 2))
    atom.setTerm(0, DefaultTermFactory.instance.createConstant("a0"))
    atom.setTerm(1, DefaultTermFactory.instance.createConstant("a1"))

    //Type(Map(tx-> atom, ty-> atom), s1 )
    Type(s1 )
  }

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

    Type( s1 )
  }



  describe("ReWriter test cases") {

    it("should contains at least 2 atoms") {
       val atoms =  ReWriter.makeGeneratingAtoms( ontology1)
        assert(atoms.size == 2)
//       println (atoms)

    }

    it("should create the canonical models from 2 atoms") {
        val canonicalModels = ReWriter.canonicalModelList(ontology1)
        assert(canonicalModels.length == 2)
        canonicalModels.foreach( atomSet => assert(atomSet.asScala.size == 4))
//        println(canonicalModels)
    }


    it("should get the epsilon "){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition= test.buildTestTreeDecomposition("src/main/resources/Q7.gml", "src/main/resources/Q7.cq")
      val s = getMockTypeEpsilon
      val atoms = new ReWriter(ontology1).makeAtoms( t.getRoot,s)
      println(atoms)
      assert( atoms.length==1 )
    }

    it("should get the anonymous "){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition= test.buildTestTreeDecomposition("src/main/resources/Q7.gml", "src/main/resources/Q7.cq")
      val s = getMockTypeAnonymous
      val atoms = new ReWriter(ontology1).makeAtoms( t.getRoot,s)
      println(atoms)
      assert( atoms.length==1 )

    }

    it("should get the empty set for mixed type   "){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition= test.buildTestTreeDecomposition("src/main/resources/Q7.gml", "src/main/resources/Q7.cq")
      val s = getMockTypeMixed
      val atoms = new ReWriter(ontology1).makeAtoms( t.getRoot,s)
      println(atoms)
      assert( atoms.length==2 )
    }

    it("should rewrite the query for ont-1"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition("src/main/resources/Q7.gml", "src/main/resources/Q7.cq")

      val result = new ReWriter(ontology1).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.size == 7 )
    }


    it("should rewrite the query for ont-2"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition("src/main/resources/Q7.gml", "src/main/resources/Q7.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontology2).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.size == 20 ) // verify this result

      val datalog: List[Clause] =  ReWriter.generateDatalog(result )
      println(s"${datalog.mkString(".\n")}.")
      assert(datalog.size==2)
    }



    it("should rewrite the query for q-3"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition("src/main/resources/Q3.gml", "src/main/resources/Q3.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontology2).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.size == 4 ) // verify this result

      val datalog=  ReWriter.generateDatalog(result )
      println(datalog.mkString(".\n"))
      assert(datalog.size==3)
    }


    it("should rewrite the query for q-3 with ont-3"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition("src/main/resources/Q3.gml", "src/main/resources/Q3.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontology3).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.size == 5 ) // verify this result

      val datalog=  ReWriter.generateDatalog(result )
      println(datalog.mkString(".\n"))
      assert(datalog.size==5)

    }

    it("should rewrite the query for q-3 with ont-4"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition("src/main/resources/Q3.gml", "src/main/resources/Q3.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontology4).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.size == 5 ) // verify this result

      val datalog=  ReWriter.generateDatalog(result )
      println(datalog.mkString(".\n"))
      assert(datalog.size==7)

    }


    it("should rewrite the query for q-3-1 with ont-4"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition("src/main/resources/Q3-1.gml", "src/main/resources/Q3-1.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontology4).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      //assert( result.size == 5 ) // verify this result

      val datalog=  ReWriter.generateDatalog(result )
      println(datalog.mkString(".\n"))
     // assert(datalog.size==7)

    }

    it("should rewrite the query for q-7-p with ont-q7p"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition("src/main/resources/Q7p.gml", "src/main/resources/Q7p.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontology5).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      //assert( result.size == 5 ) // verify this result

      val datalog=  ReWriter.generateDatalog(result )
      println(datalog.mkString(".\n"))
      // assert(datalog.size==7)

    }

  }

  describe( "Substitution tests cases"){
    it("should be a OntologyTerm instance"){
      assert( TypeTermFactory.createOntologyVariable("X1").isInstanceOf[OntologyTerm] )
      assert( TypeTermFactory.createOntologyConstant(ConstantType.EPSILON ).isInstanceOf[OntologyTerm])
      assert( TypeTermFactory.createOntologyLiteral("test").isInstanceOf[OntologyTerm])
      assert( TypeTermFactory.createOntologyLiteral(new DefaultURI("http://www.dcs.bbk.ac.uk/~srapis01/ont/#test"), "test")
        .isInstanceOf[OntologyTerm])


    }

    it("should be a QueryTerm instance"){
      assert( TypeTermFactory.createQueryVariable("X1").isInstanceOf[QueryTerm] )
      assert( TypeTermFactory.createQueryConstant(ConstantType.EPSILON ).isInstanceOf[QueryTerm])
      assert( TypeTermFactory.createQueryLiteral("test").isInstanceOf[QueryTerm])
      assert( TypeTermFactory.createQueryLiteral(new DefaultURI("http://www.dcs.bbk.ac.uk/~srapis01/ont/#test"), "test")
        .isInstanceOf[QueryTerm])


    }


//    it("should substitute x with x6 in the clause: p9[1](X6) :- a[1](X), EQ[2](X6,X) "){
//
//      val x6:Term=  QueryTerm( DefaultTermFactory.instance().createVariable("X6") )
//      val x:Term=  OntologyTerm( DefaultTermFactory.instance().createVariable("X") )
//
//
//
//      val head = new DefaultAtom( new Predicate("p9", 1 )  ,  List(x6).asJava )
//      val atomA = new DefaultAtom( new Predicate("a", 1 )  ,  List(x).asJava )
//      val equality = Equality(x6,x)
//      val body:List[Atom] = List(atomA, equality)
//
//      val clause = Clause(head, body)
//
//      ReWriter.generateDatalog()
//
//    }

  }


}


