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

import java.io.{BufferedWriter, File, FileWriter}

import fr.lirmm.graphik.graal.api.core.{Predicate, Rule, Term}
import fr.lirmm.graphik.graal.core.ruleset.LinkedListRuleSet
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import fr.lirmm.graphik.graal.core.{DefaultAtom, TreeMapSubstitution}
import fr.lirmm.graphik.graal.io.dlp.DlgpParser
import fr.lirmm.graphik.util.DefaultURI
import org.scalatest.FunSpec

import scala.collection.JavaConverters._
import scala.io.Source

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
  private val pathToBenchmark100 = "src/main/resources/benchmark/100"
  private val pathToBenchmark300 = "src/main/resources/benchmark/300"
  private val pathToBenchmarkNDL_SQL = "src/main/resources/benchmark/NDL-SQL"
  private val ontology1 = getOntology("src/main/resources/ont-1.dlp")
  private val ontology2 = getOntology("src/main/resources/ont-2.dlp")
  private val ontology3 = getOntology("src/main/resources/ont-3.dlp")
  private val ontology4 = getOntology("src/main/resources/ont-4.dlp")
  private val ontology5 = getOntology("src/main/resources/ont-5.dlp")
  private val ontCar = getOntology("src/main/resources/ont-car.dlp")
  private val ontBenchmark100Dep = getOntology(s"$pathToBenchmark100/dependencies/deep.t-tgds.dlp")
  private val ontBenchmark100sDep = getOntology(s"$pathToBenchmark100/dependencies/deep.st-tgds.dlp")
  private val ontBenchmark300Dep = getOntology(s"$pathToBenchmark300/dependencies/deep.t-tgds.dlp")
  private val ontBenchmarkNDL_SQL = getOntology(s"$pathToBenchmarkNDL_SQL/dependencies/15-tw.dlp")
  ///private val ontBenchmark300Dep = getOntology(s"${pathTo300Dep}deep.t-tgds.dlp")

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
        assert(atoms.lengthCompare(2) == 0)
//      println (atoms)
    }

    it("should create the canonical models from 2 atoms") {
        val canonicalModels = ReWriter.canonicalModelList(ontology1)
        assert(canonicalModels.lengthCompare(2) == 0)
        canonicalModels.foreach( atomSet => assert(atomSet.asScala.size == 4))
//      println(canonicalModels)
    }


    it("should get the epsilon "){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition= test.buildTestTreeDecomposition("src/main/resources/Q7.gml", "src/main/resources/Q7.cq")
      val s = getMockTypeEpsilon
      val atoms = new ReWriter(ontology1).makeAtoms( t.getRoot,s)
      println(atoms)
      assert( atoms.lengthCompare(1) == 0 )
    }

    it("should get the anonymous "){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition= test.buildTestTreeDecomposition("src/main/resources/Q7.gml", "src/main/resources/Q7.cq")
      val s = getMockTypeAnonymous
      val atoms = new ReWriter(ontology1).makeAtoms( t.getRoot,s)
      println(atoms)
      assert( atoms.lengthCompare(1) == 0 )

    }

    it("should get the empty set for mixed type   "){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition= test.buildTestTreeDecomposition("src/main/resources/Q7.gml", "src/main/resources/Q7.cq")
      val s = getMockTypeMixed
      val atoms = new ReWriter(ontology1).makeAtoms( t.getRoot,s)
      println(atoms)
      assert( atoms.lengthCompare(2) == 0 )
    }

    it("should rewrite the query for ont-1"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition("src/main/resources/Q7.gml", "src/main/resources/Q7.cq")

      val result = new ReWriter(ontology1).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.lengthCompare(7) == 0 )
    }


    it("should rewrite the query for ont-2"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition("src/main/resources/Q7.gml", "src/main/resources/Q7.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontology2).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.lengthCompare(20) == 0 ) // verify this result

      val datalog: List[Clause] =  ReWriter.generateDatalog(result )
      println(s"${datalog.mkString(".\n")}.")
      assert(datalog.lengthCompare(2) == 0)
    }



    it("should rewrite the query for q-3"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition("src/main/resources/Q3.gml", "src/main/resources/Q3.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontology2).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.lengthCompare(4) == 0 ) // verify this result

      val datalog=  ReWriter.generateDatalog(result )
      println(datalog.mkString(".\n"))
      assert(datalog.lengthCompare(3) == 0)
    }


    it("should rewrite the query for q-3 with ont-3"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition("src/main/resources/Q3.gml", "src/main/resources/Q3.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontology3).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.lengthCompare(5) == 0 ) // verify this result

      val datalog=  ReWriter.generateDatalog(result )
      println(datalog.mkString(".\n"))
      assert(datalog.lengthCompare(5) == 0)

    }

    it("should rewrite the query for q-3 with ont-4"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition("src/main/resources/Q3.gml", "src/main/resources/Q3.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontology4).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.lengthCompare(5) == 0 ) // verify this result

      val datalog=  ReWriter.generateDatalog(result )
      println(datalog.mkString(".\n"))
      assert(datalog.lengthCompare(7) == 0)

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
      assert( result.lengthCompare(63) == 0 ) // verify this result

      val datalog=  ReWriter.generateDatalog(result )
      println(datalog.mkString(".\n"))
      assert(datalog.lengthCompare(13) == 0)

    }


    it("should rewrite the query for Q-car with ont-car"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition("src/main/resources/Q-car.gml", "src/main/resources/Q-car.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontCar).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.lengthCompare(2) == 0 ) // verify this result

      val datalog=  ReWriter.generateDatalog(result )
      println(datalog.mkString(".\n"))
      assert(datalog.lengthCompare(2) == 0)

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

  }

  describe( "Benchmark tests cases"){

     def transform2Dlp(pin:String, pout:String ): Unit ={
      val lines = Source.fromFile(pin).getLines.toList

      val datalog = lines.map( line => {
        val sentence = line.split("->")
        val head = sentence(1).replace("?", "").replace(".", "").trim
        val body = sentence(0).replace("?", "").trim
        s"$head :- $body."

      })

       assert( lines.lengthCompare(datalog.length) == 0)

      val text = datalog.mkString("\n")
      val out = new BufferedWriter(new FileWriter(pout))
      out.write(text)
      out.close()
    }

    def transformCQ (pin:String, pout:String ): Unit ={
      val cq = Source.fromFile(pin).getLines().map(_.trim).reduce(_ + _)
      val text = cq.replace("<-", ":- ").replace("\n", " ").replace("?","").trim
      val out = new BufferedWriter(new FileWriter(pout))
      out.write(text)
      out.close()
    }


//
    it("should rewrite the benchmark 100 tgd deep.t-tgds.txt. "){
      transform2Dlp(s"$pathToBenchmark100/dependencies/deep.t-tgds.txt",
        s"$pathToBenchmark100/dependencies/deep.t-tgds-test.dlp")
    }

    it("should rewrite the benchmark 100/1000 tgd deep.st-tgds.txt."){

      transform2Dlp(s"$pathToBenchmark100/dependencies/deep.st-tgds.txt",
        s"$pathToBenchmark100/dependencies/deep.st-tgds-test.dlp")
    }

    it("should rewrite the benchmark cq q01.txt to q01.cq"){

      transformCQ(s"$pathToBenchmark100/queries/q01.txt",
        s"$pathToBenchmark100/queries/q01-test.cq")
    }

    it("should rewrite the benchmark 300 tgd deep.t-tgds.txt. "){
      transform2Dlp(s"$pathToBenchmark100/dependencies/deep.t-tgds.txt",
        s"$pathToBenchmark300/dependencies/deep.t-tgds-test.dlp")
    }

    it("should rewrite the query for q01 with ont of deep.t-tdgs.dlp"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition(s"$pathToBenchmark100/queries/q01.gml",
        s"$pathToBenchmark100/queries/q01.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontBenchmark100Dep).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.lengthCompare(3) == 0 ) // verify this result

      val datalog=  ReWriter.generateDatalog(result )
      println(datalog.mkString(".\n"))
      assert(datalog.lengthCompare(3) == 0)

    }

    it("should rewrite the query for q01 with ont of deep.st-tdgs.dlp"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition(s"$pathToBenchmark100/queries/q01.gml",
        s"$pathToBenchmark100/queries/q01.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontBenchmark100sDep).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.lengthCompare(94) == 0 ) // verify this result

      val datalog=  ReWriter.generateDatalog(result )
      println(datalog.mkString(".\n"))
      //assert(datalog.size==3)

    }

    it("should rewrite  for queries  with 300 ont of deep.t-tdgs.dlp"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition(s"$pathToBenchmark300/queries/queries.gml",
        s"$pathToBenchmark300/queries/queries.cq")

      val result: Seq[RuleTemplate] = new ReWriter(ontBenchmark300Dep).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
      println(result)
      assert( result.lengthCompare(80) == 0 ) // verify this result

      val datalog=  ReWriter.generateDatalog(result )
      println(datalog.mkString(".\n"))
      assert(datalog.lengthCompare(11) == 0)

    }


    it("should generate Flink script"){
      val test:TreeDecompositionTest  = new TreeDecompositionTest
      val t:TreeDecomposition = test.buildTestTreeDecomposition(s"$pathToBenchmark300/queries/queries.gml",
        s"$pathToBenchmark300/queries/queries.cq")

      val datalog=  ReWriter.generateDatalog(
        new ReWriter(ontBenchmark300Dep).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t)) )

      val res = ReWriter.generateFlinkScript(datalog)
      assert(res!=null)

    }


    it("should rewrite the query for queries for NDL-SQL"){
//      val test:TreeDecompositionTest  = new TreeDecompositionTest


      assert(!ontBenchmarkNDL_SQL.isEmpty)
//      val t:TreeDecomposition = test.buildTestTreeDecomposition(s"$pathToBenchmarkNDL_SQL/queries/queries.gml",
      // s"$pathToBenchmarkNDL_SQL/queries/queries.cq")
//
//      val result: Seq[RuleTemplate] = new ReWriter(ontBenchmark300Dep).generateRewriting(Type(new TreeMapSubstitution()) , Splitter(t))
//      println(result)
//      assert( result.size == 80 ) // verify this result
//
//      val datalog=  ReWriter.generateDatalog(result )
//      println(datalog.mkString(".\n"))
//      assert(datalog.size== 11)

    }

    it( "has to read a clause"){
      val cq = Source.fromFile(s"$pathToBenchmark100/queries/q01-t.cq").getLines.reduce(_+_)

      val headBody = cq.split(":-")

      assert(headBody.length==2)
      val dlgpParser = new DlgpParser(new File(s"$pathToBenchmark100/queries/q01-t.cq"))
      val rules = dlgpParser.asScala.toList.map {
        case rule: Rule =>
          rule
      }

      println(rules)


      val vertices =  rules.flatMap(r => r.getHead.getTerms.asScala.toList ::: r.getBody.getTerms.asScala.toList).distinct

      println( vertices.map( v=> s"vertex($v)").mkString(".\n" ) )

      def getpairs ( terms : List[Term] ) = for ( t1 <-  terms; t2 <-  terms; if t1 != t2 ) yield  (t1, t2)

      val edges  =rules.head.getBody.asScala.toList.flatMap( a=> getpairs(a.getTerms.asScala.toList) )     // getTerms.asScala.toList



//      val edges =
//        for ( t1 <-  terms; t2 <-  terms; if t1 != t2 ) yield  (t1, t2)


       print ( edges.map( h => s"edge$h"  ).mkString(".\n" ) )
    }

  }

}


