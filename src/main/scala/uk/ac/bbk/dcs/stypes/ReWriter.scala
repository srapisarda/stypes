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

import java.io.File
import java.util.UUID

import fr.lirmm.graphik.graal.api.core._
import fr.lirmm.graphik.graal.core.{DefaultAtom, DefaultRule}
import fr.lirmm.graphik.graal.core.atomset.graph.DefaultInMemoryGraphAtomSet
import fr.lirmm.graphik.graal.forward_chaining.DefaultChase
import fr.lirmm.graphik.graal.io.dlp.DlgpParser

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.io.Source

/**
  * Created by
  * Salvatore Rapisarda
  * Stanislav Kikot
  *
  * on 02/05/2017.
  */
object ReWriter {

  /**
    * Generates a set of generating atoms
    *
    * @param ontology a RuleSet
    * @return List[Atom]
    */
  def makeGeneratingAtoms(ontology: List[Rule], includeAllAtoms:Boolean=false): List[Atom] = {

    @tailrec
    def visitRuleSet(rules: List[Rule], acc: List[Atom]): List[Atom] = rules match {
      case List() => acc
      case x :: xs =>
        if (includeAllAtoms || containsExistentialQuantifier(x) ) visitRuleSet(xs, acc ::: x.getBody.asScala.toList)
        else visitRuleSet(xs, acc)
    }

    def containsExistentialQuantifier(rule: Rule): Boolean = {
      val headTerms = rule.getHead.getTerms.asScala.toList
      val bodyTerms = rule.getBody.getTerms.asScala.toList

      // headTerms.size > bodyTerms.size

      def checkHelper(headTerms: List[Term]): Boolean = headTerms match {
        case List() => false
        case x :: xs => if (!bodyTerms.contains(x)) true else checkHelper(xs)

      }

      checkHelper(headTerms)

    }

    visitRuleSet(ontology, List())

  }

  /**
    * Creates additional rules form an existing [[List]] of [[Rule]]
    *
    * @param ontology which contains a [[List]] of [[Rule]]
    * @return a [[List]] of [[Rule]]
    */
  def createAdditionalRules(ontology: List[Rule]): List[Rule] ={
    val generatingAtoms = makeGeneratingAtoms(ontology, includeAllAtoms = true)

    def collectRules( generatingAtom : Atom ): List[Rule] = {
      val canonicalModel: AtomSet = canonicalModelList(ontology, List(generatingAtom)).head
      for (atom <- canonicalModel.asScala.toList; if atom!=generatingAtom && !atom.getTerms.asScala.exists(isAnonymous))
        yield {
          val rule = new DefaultRule
          val body = new DefaultInMemoryGraphAtomSet()
          body.add(generatingAtom)
          rule.setBody(body)
          val head = new DefaultInMemoryGraphAtomSet()
          head.add(atom)
          rule.setHead(head)
          rule
        }
    }
    generatingAtoms.flatMap(collectRules)
  }


  /**
    * Generates canonical models for each of the generating atoms
    *
    * @param ontology a RuleSet
    * @return a List[AtomSet]
    */
  def canonicalModelList(ontology: List[Rule]): List[AtomSet] =
    canonicalModelList(ontology, makeGeneratingAtoms(ontology))

  private def canonicalModelList(ontology: List[Rule], generatingAtomList: List[Atom]): List[AtomSet] = {

    @tailrec
    def canonicalModelList(generatingAtomList: List[Atom], acc: List[AtomSet]): List[AtomSet] = generatingAtomList match {
      case List() => acc.reverse
      case x :: xs => canonicalModelList(xs, buildChase(x) :: acc)
    }

    def buildChase(atom: Atom): AtomSet = {
      val store: AtomSet = new DefaultInMemoryGraphAtomSet
      store.add(atom)
      val chase = new DefaultChase(ontology.asJava, store)
      chase.execute()

      store
    }

    canonicalModelList(generatingAtomList, List())
  }

  def isAnonymous(x: Term): Boolean =
    x.getLabel.toLowerCase.startsWith("ee")

  case class TermsTupleList(value: List[(Term, Term)])

  def generateDatalog(rewriting: Seq[RuleTemplate]): List[Clause] = {

    def getAtomsFromRewrite(ruleTemplate: RuleTemplate, map: Map[Int, Int], currentIndex: Int): (List[Clause], Map[Int, Int], Int) = {

      def transformAtom(atom: Atom, map: Map[Int, Int], currentIndex: Int): (Atom, Map[Int, Int], Int) =
        atom.getPredicate.getIdentifier match {
          case _: String => (atom, map, currentIndex)
          case atomIdentifier: Any =>
            val predicateHash = atomIdentifier.toString.hashCode()
            val next =
              if (map.contains(predicateHash)) (map, map(predicateHash), currentIndex)
              else {
                val plusOne = currentIndex + 1
                (map + (predicateHash -> plusOne), plusOne, plusOne)
              }
            (new DefaultAtom(DatalogPredicate(s"p${next._2}", atom.getPredicate.getArity, next._2 == 1), atom.getTerms), next._1, next._3)
        }

      @tailrec
      def transformBody(body: Any, acc: (List[Any], Map[Int, Int], Int)): (List[Any], Map[Int, Int], Int) =
        body match {
          case List() => acc
          case x :: xs => x match {
            case equ: List[Any] =>
              transformBody(xs, (equ :: acc._1, acc._2, acc._3))
            case bodyAtom: Atom =>
              val res = transformAtom(bodyAtom, acc._2, acc._3)
              transformBody(xs, (res._1 :: acc._1, res._2, res._3))
          }
        }

      val head: (Atom, Map[Int, Int], Int) = transformAtom(ruleTemplate.head, map, currentIndex)
      val bodyWithTransformedAtomsButWithListListPairForEqualities: (List[Any], Map[Int, Int], Int) =
        transformBody(ruleTemplate.body, (List(), head._2, head._3))

      val clauses: List[Clause] = OpenUpBrackets(bodyWithTransformedAtomsButWithListListPairForEqualities._1).map(b => Clause(head._1, b))

      (clauses, bodyWithTransformedAtomsButWithListListPairForEqualities._2, bodyWithTransformedAtomsButWithListListPairForEqualities._3)
    }

    def EqualityAtom(e: (Term, Term)): Atom = {
      Equality(e._1, e._2).getAtom
    }

    def EqualityAtomConjunction(list: List[(Term, Term)]): List[Atom] = {
      list.map(equality => EqualityAtom(equality))
    }

    def OpenUpBrackets(body: List[Any], acc: List[List[Atom]] = List()): List[List[Atom]] = {
      body match {
        case List() => List()
        case head :: tail => head match {
          case x: Atom =>
            if (tail.isEmpty) List(List(x))
            else OpenUpBrackets(tail).map(a => x :: a)
          case x: Seq[Any] =>
            x.head match {
              case y: Seq[Any] =>
                y.head match {
                  case _@(_: Term, _: Term) =>
                    if (tail.isEmpty)
                      x.toList.map(coe => EqualityAtomConjunction(coe.asInstanceOf[Seq[(Term, Term)]].toList))
                    else
                      cartesianProduct(x.asInstanceOf[Seq[Seq[(Term, Term)]]], OpenUpBrackets(tail))
                }
            }
        }
      }
    }

    def cartesianProduct(x: Seq[Seq[(Term, Term)]], res: List[List[Atom]]): List[List[Atom]] = {
      x.flatMap(equalityConj => res.map((t: List[Atom]) => EqualityAtomConjunction(equalityConj.toList) ++ t)).toList
    }

    def visitRewriting(rewriting: List[RuleTemplate], acc: (List[List[Clause]], Map[Int, Int], Int)):
    (List[List[Clause]], Map[Int, Int], Int) =
      rewriting match {
        case List() => acc
        case x :: xs =>
          val res = getAtomsFromRewrite(x, acc._2, acc._3)
          visitRewriting(xs, (res._1 :: acc._1, res._2, res._3))
      }

    def removeEmptyClauses(datalog: List[Clause]): List[Clause] = {

      def removalHelper(datalog: List[Clause]): (List[Clause], Boolean) = {
        val defined = datalog.map(_.head.getPredicate).toSet

        def remove(l: List[Atom]): Boolean = l match {
          case List() => false
          case x :: xs => x.getPredicate match {
            case _: DatalogPredicate =>
              !defined.contains(x.getPredicate) || remove(xs)
            case _ =>
              remove(xs)
          }
        }

        val ret = datalog.
          filter(rule => !remove(rule.body))

        (ret, ret.lengthCompare(datalog.size) != 0)
      }

      val removalOutcome = removalHelper(datalog)

      val iterate = removalOutcome._2

      val removalResult = removalOutcome._1

      if (iterate)
        removalHelper(removalResult)._1
      else
        removalResult
    }

    def removeDuplicate(datalog: List[Clause]): List[Clause] = datalog.toSet.toList

    def predicateSubstitution(datalog: List[Clause]): List[Clause] = {

      val goalPredicate: Predicate = datalog.filter(p => p.head.getPredicate.isInstanceOf[DatalogPredicate])
        .find(p => p.head.getPredicate.asInstanceOf[DatalogPredicate].isGoalPredicate)
        .get.head.getPredicate

      val toBeSubstituted = datalog
        .sortBy(_.head.getPredicate)
        .map(p => (p.head.getPredicate, 1L))
        .groupBy(_._1)
        .filter(c => c._1 != goalPredicate && c._2.lengthCompare(1) == 0)
        .keys.toList

      val substitutionTable: Map[Atom, List[Atom]] = datalog
        .filter(p => toBeSubstituted.contains(p.head.getPredicate))
        .map(p => p.head -> p.body).toMap

      def visitPredicates(datalog: List[Atom], acc: (List[Atom], Boolean)): (List[Atom], Boolean) = datalog match {
        case List() => (acc._1.reverse, acc._2)
        case atom :: xs => atom.getPredicate match {
          case _: DatalogPredicate =>
            if (substitutionTable.contains(atom))
              visitPredicates(xs, (substitutionTable(atom) ::: acc._1, true))
            else
              visitPredicates(xs, (atom :: acc._1, acc._2))
          case _ =>
            visitPredicates(xs, (atom :: acc._1, acc._2))
        }
      }

      def substitution(datalog: List[Clause]): List[Clause] = {
        def substitutionH(datalog: List[Clause]): List[(Clause, Boolean)] = {
          datalog.map(d => {
            val visit = visitPredicates(d.body, (List(), false))
            (Clause(d.head, visit._1), visit._2)
          })
        }

        val sub = substitutionH(datalog)

        val hasSubstitution: Boolean = sub.map(_._2).reduce((s1, s2) => s1 || s2)

        if (hasSubstitution)
          substitution(sub.map(_._1))
        else
          sub.map(_._1)
      }

      substitution(datalog)
        .filter(p => !toBeSubstituted.contains(p.head.getPredicate))

    }

    def equalitySubstitution(datalog: List[Clause]): List[Clause] = {

      def equalityClauseSubstitution(clause: Clause): Clause = {

        def existSubstitutionTerm(list: List[(Term, Term)], term: Term): Boolean = list match {
          case List() => false
          case x :: xs => if (x._1.equals(term)) true
          else existSubstitutionTerm(xs, term)
        }

        def createSubstitutionList(body: List[Atom], acc: List[(Term, Term)] = List()): List[(Term, Term)] = body match {
          case List() => acc
          case x :: xs => x match {
            case atom: Equality =>

              if (atom.t1.isInstanceOf[QueryTerm] && atom.t2.isInstanceOf[OntologyTerm])
                if (existSubstitutionTerm(acc, atom.t2))
                  createSubstitutionList(xs, acc)
                else
                  createSubstitutionList(xs, (atom.t2, atom.t1) :: acc)

              else if (atom.t2.isInstanceOf[QueryTerm] && atom.t1.isInstanceOf[OntologyTerm])
                if (existSubstitutionTerm(acc, atom.t1))
                  createSubstitutionList(xs, (atom.t1, atom.t2) :: acc)
                else
                  createSubstitutionList(xs, acc)
              else if (atom.t1.isInstanceOf[QueryTerm] && atom.t2.isInstanceOf[QueryTerm] && !atom.t1.equals(atom.t2))
                createSubstitutionList(xs, (atom.t1, atom.t2) :: acc)

              else
                createSubstitutionList(xs, acc)

            case _ => createSubstitutionList(xs, acc)
          }
        }

        def removeEqualityWithSameTerms(atoms: List[Atom], acc: List[Atom]): List[Atom] = atoms match {
          case List() => acc
          case x :: xs => x match {
            case atom: Equality =>
              if (atom.t1.equals(atom.t2)) removeEqualityWithSameTerms(xs, acc)
              else removeEqualityWithSameTerms(xs, x :: acc)
            case _ => if (!acc.contains(x)) removeEqualityWithSameTerms(xs, x :: acc) else removeEqualityWithSameTerms(xs, acc)
          }
        }

        // getting substitutionSet σ
        val substitutionSetMap: Map[Term, Term] = createSubstitutionList(clause.body)
          .toSet
          .toMap

        def getSubstitutionTerm(t: Term): Term = if (substitutionSetMap.contains(t)) substitutionSetMap(t) else t

        def substituteAtom(atom: Atom) = atom match {
          case _: Equality =>
            val terms = atom.getTerms.asScala.toList.map(getSubstitutionTerm).toVector
            if (terms.size == 2)
              Equality(terms(0), terms(1))
            else atom
          case _ =>
            new DefaultAtom(atom.getPredicate,
              atom.getTerms.asScala.toList.map(getSubstitutionTerm).asJava)
        }

        val head = substituteAtom(clause.head)

        val body = removeEqualityWithSameTerms(clause.body.map(substituteAtom), List())

        Clause(head, body)

        if (substitutionSetMap.isEmpty) Clause(head, body)
        else equalityClauseSubstitution(Clause(head, body))

      }

      datalog.map(equalityClauseSubstitution)
    }

    val datalog = visitRewriting(rewriting.toList, (List(), Map(), 0))._1.flatten.reverse

    if (datalog.isEmpty) {
      datalog
    }
    else {
      val removalResult: List[Clause] = removeEmptyClauses(removeDuplicate(datalog))
      val predicateSubstitutionRes: List[Clause] = predicateSubstitution(removalResult)
      equalitySubstitution(predicateSubstitutionRes)
    }
  }

  def generateFlinkScript(datalog: List[Clause], dataSources: Map[String, String]): String = {

    lazy val unknownData = "@@unknownData@@"

    def getScriptFromSameHeadClauses(clauses: List[Clause], matchedDataSources: Map[Predicate, String]):
    (Map[Predicate, String], String) = {

      def mapPositionTerm(terms: Seq[Term], acc: Map[Term, List[Int]] = Map(), index: Int = -1): Map[Term, List[Int]] = terms match {
        case List() => acc
        case x :: xs =>
          val idx = index + 1
          val mappedTerm = if (acc.contains(x)) x -> (idx :: acc(x)).reverse else x -> List(idx)
          mapPositionTerm(xs, acc + mappedTerm, idx)
      }

      def getProjection(head: Atom,  lhsTermsPosMap:Map[Term, List[Int]],  rhsTermsPosMap: Map[Term, List[Int]]): (List[Term], String) = {
        def projectAtLeastTuple2( proj: List[String] ) = if (proj.lengthCompare(1)==0)  List(proj.head, proj.head) else proj

        val terms: List[Term] = (lhsTermsPosMap.keys ++ rhsTermsPosMap.keys).toList
        val termsToProject = getTermsToProject( head, terms  )
        val projection =
          if ( termsToProject.isEmpty )
            (List(), "")
          else{

            if ( rhsTermsPosMap.isEmpty){
              val lhsProjection = termsToProject.get
                .filter(term => lhsTermsPosMap.contains(term))
                .map(term => lhsTermsPosMap(term).head)
                .map(p => s"t._${p + 1}")

              (termsToProject.get, s".map(t=> (${projectAtLeastTuple2(lhsProjection).mkString(",")}))")

            }else {
              val rhsTermsNotInLhl = rhsTermsPosMap
                .filter(p => !lhsTermsPosMap.contains(p._1))

              val lhsProjection = termsToProject.get
                .filter(term => lhsTermsPosMap.contains(term))
                .map(term => lhsTermsPosMap(term).head)
                .map(p => s"t._1._${p + 1}")

              val rhsTermsProjection = termsToProject.get
                .filter(term => rhsTermsPosMap.contains(term))
                .map(term => rhsTermsNotInLhl(term).head)
                .map(p => s"t._2._${p + 1}")

              (termsToProject.get, s".map(t=> (${projectAtLeastTuple2(lhsProjection ++ rhsTermsProjection).mkString(",")}))")
            }
          }
        projection
      }

      def visitClauseBody(atoms: List[Atom], head: Atom, acc: (Map[Predicate, String], String),
                          lhs: Option[Atom] = None): (Map[Predicate, String], String) = atoms match {
        case List() => acc;
        case rhs :: xs =>
          val matchedDataSources = acc._1
          val query = acc._2

          val ds =
            if (!matchedDataSources.contains(rhs.getPredicate)) {
              if (dataSources.contains(rhs.getPredicate.getIdentifier.toString))
                matchedDataSources + (rhs.getPredicate -> dataSources(rhs.getPredicate.getIdentifier.toString))
              else matchedDataSources + (rhs.getPredicate -> unknownData)
            } else matchedDataSources

          if (lhs.isEmpty) {
            val mappedTerms =  getProjection(head,  mapPositionTerm(rhs.getTerms.asScala.toList), Map() )
            visitClauseBody(xs, head, (ds, rhs.getPredicate.getIdentifier.toString + mappedTerms._2), Some(rhs))
          } else {
            val lhsTermsPosMap = mapPositionTerm(lhs.get.getTerms.asScala.toList)
            val rhsTermsPosMap = mapPositionTerm(rhs.getTerms.asScala.toList)
            val commonPairs: List[(Int, Int)] =
              (for (rt <- rhsTermsPosMap; if lhsTermsPosMap.contains(rt._1))
                yield
                  for (l <- lhsTermsPosMap(rt._1); r <- rt._2)
                    yield (l, r)).toList.flatten


            // check for arity on the head
            val postBooleanCondition =  if (head.getPredicate.getArity>0 ) "" else ".count()>0"
            // condition
            val queryConditions =
              if (commonPairs.isEmpty) ""
              else s".where(${commonPairs.map(_._1).mkString(",")}).equalTo(${commonPairs.map(_._2).mkString(",")})$postBooleanCondition"
            // projection
            val mappedTerms =  getProjection(head, lhsTermsPosMap, mapPositionTerm(rhs.getTerms.asScala.toList) )
            // new relation
            val mergedAtom = new DefaultAtom(new Predicate(UUID.randomUUID().toString, mappedTerms._1.size),
              mappedTerms._1.asJava)

            visitClauseBody(xs, head,
              (ds, s"$query.join(${rhs.getPredicate.getIdentifier})$queryConditions${mappedTerms._2}"), Some(mergedAtom))
          }

      }

      def getTermsToProject( head:Atom, bodyTerms: => List[Term]) = {
        if (head.getPredicate.getArity > 0) {
          Some(bodyTerms.filter(p => head.getTerms.asScala
            .map(_.getIdentifier.toString).contains(p.getIdentifier.toString)).distinct)
        } else None
      }

      def getUnionScript(clauses: List[Clause], acc: (Map[Predicate, String], List[String])):
      (Map[Predicate, String], List[String]) =
        clauses match {
          case List() => acc
          case x :: xs =>
            val queryClause: (Map[Predicate, String], String) = visitClauseBody(x.body, x.head, (acc._1, ""))
            val scripts = acc._2
            val map: Map[Predicate, String] = acc._1 ++ queryClause._1
            getUnionScript(xs, (map, s"${queryClause._2}" :: scripts))

        }

      val scriptsAndDataSources = getUnionScript(clauses, (matchedDataSources, List()))
      lazy val binaryOperator = if ( clauses.head.head.getPredicate.getArity > 0 ) "union" else "||"
      (scriptsAndDataSources._1, scriptsAndDataSources._2.reduce((a1, a2) => s"($a1 $binaryOperator $a2)\n"))
    }

    def mapPredicateGroups(grouped: List[(Predicate, List[Clause])], acc: (Map[Predicate, String], List[String]) = (Map(), List()))
    : (Map[Predicate, String], List[String]) = grouped match {
      case List() => acc
      case x :: xs =>
        val script = getScriptFromSameHeadClauses(x._2, acc._1)
        val map: Map[Predicate, String] = acc._1 ++ script._1
        mapPredicateGroups(xs, (map, s"private lazy val ${x._1.getIdentifier}= ${script._2}" :: acc._2))

    }

    val grouped: List[(Predicate, List[Clause])] = datalog.groupBy(p => p.head.getPredicate).toList
    val result = mapPredicateGroups(grouped)

    //    grouped.map(clause => s"lazy val ${clause._1.getIdentifier} = ${getScriptFromSameHeadClauses(clause._2, Map())._2}")
    //      .mkString("\n")

    "import org.apache.flink.api.scala._\nimport org.apache.flink.configuration.Configuration\n\nobject FlinkRewriter extends App {\n\n" +
      Source.fromFile("src/main/resources/flinker-head.txt").mkString +
      "\n\n//DATA\n" +
      result._1
        // filter all predicates already defined as head in any declared clause
        // because it is not necessary to declare something that will be declared later
        .filter(p =>
        !datalog.exists(
          clause => clause.head.getPredicate.getIdentifier == p._1.getIdentifier))
        // data mapping
        .map(p => {
        val variable = s"private lazy val ${p._1.getIdentifier.toString}  = "

        val data =
          if (p._2 == unknownData)
            s"unknownData${p._1.getArity}" else "env.readTextFile(\"" + p._2 + "\")" + s".map(stringMapper${p._1.getArity})"

        variable + data
      })
        //Rewriting
        .mkString("\n") + "\n\n//Rewriting\n" + result._2.mkString("\n") +
      "\n\n}"

  }

  def getOntology(filename: String): List[Rule] = {
    for ( rule <-  new DlgpParser(new File(filename)).asScala.toList; if rule.isInstanceOf[Rule] )
      yield rule.asInstanceOf[Rule]
  }

  def getDatalogRewriting(filename: String): List[Clause] = {
    val dlgpParser = new DlgpParser(new File(filename))
    val clauses = dlgpParser.asScala.toList
    clauses.map {
      case rule: Rule =>
        val head: Atom = {
          val atom = rule.getHead.asScala.head
          if (atom.getTerms.size == 1 &&
            (atom.getTerm(0).getType.equals(Term.Type.LITERAL) ||
              atom.getTerm(0).getType.equals(Term.Type.CONSTANT)))
            new DefaultAtom(new Predicate(atom.getPredicate.getIdentifier.toString, 0))
          else atom
        }
        val body: List[Atom] = rule.getBody.asScala.toList
        Clause(head, body)
    }
  }

}

class ReWriter(ontology: List[Rule]) {

  val generatingAtoms: List[Atom] = ReWriter.makeGeneratingAtoms(ontology)
  println(s"generating  canonical models")
  val t1: Long = System.nanoTime()
  val canonicalModels: List[AtomSet] = ReWriter.canonicalModelList(ontology, generatingAtoms)
  println(s"elapsed time for generating the canonical models: ${(System.nanoTime() - t1) / 1000000}ms")
  private val arrayGeneratingAtoms = generatingAtoms.toVector

  /**
    * Given a type t defined on a bag, it computes the formula At(t)
    *
    * @param bag     a Bag
    * @param theType a Type
    * @return a List[Atom]
    */
  def makeAtoms(bag: Bag, theType: Type): List[Any] = {

    @tailrec
    def getEqualities(variables: List[Term], constants: List[Term], acc: List[(Term, Term)]): List[(Term, Term)] = constants match {
      case List() => acc
      case x :: xs =>
        if (ReWriter.isAnonymous(x)) getEqualities(variables.tail, xs, acc)
        else getEqualities(variables.tail, xs, (QueryTerm(variables.head), OntologyTerm(x)) :: acc)

    }

    def isMixed(terms: List[Term]): Boolean =
      atLeastOneTerm(terms, x => ReWriter.isAnonymous(x)) && atLeastOneTerm(terms, x => !ReWriter.isAnonymous(x))

    def atLeastOneTerm(terms: List[Term], f: Term => Boolean): Boolean = terms match {
      case List() => false
      case x :: xs => if (f(x)) true else atLeastOneTerm(xs, f)
    }

    def getTypedAtom(atom: Atom, f: Term => Term): Atom = {
      val terms: List[Term] = atom.getTerms.asScala.map(f).toList
      new DefaultAtom(atom.getPredicate, terms.asJava)
    }

    @tailrec
    def visitBagAtoms(atoms: List[Atom], acc: List[Any]): List[Any] = atoms match {
      case List() => acc.reverse
      case currentAtom :: xs =>
        // All epsilon
        if (theType.areAllEpsilon(currentAtom)) visitBagAtoms(xs, getTypedAtom(currentAtom, QueryTerm) :: acc)
        // All anonymous
        else if (theType.areAllAnonymous(currentAtom)) {
          theType.homomorphism.createImageOf(currentAtom.getTerm(0)) match {
            case constantType: ConstantType =>
              val index = constantType.identifier._1
              if (index < arrayGeneratingAtoms.length) {
                val atom = arrayGeneratingAtoms(index)
                visitBagAtoms(xs, getTypedAtom(atom, OntologyTerm) :: acc)
              } else {
                visitBagAtoms(xs, acc)
              }

            case _ => visitBagAtoms(xs, new Exception("It must be a constant type!!!") :: acc)

          }

          //val atom: Option[Atom] =   // theType.genAtoms.get(currentAtom.getTerm(0))

          //if (atom.isDefined) visitBagAtoms(xs, atom.get :: acc)
          //else visitBagAtoms(xs, acc)
        }
        // mixing case
        else {
          val index = theType.getFirstAnonymousIndex(currentAtom)
          if (index < 0)
            throw new Exception("Can't get first anonymous individual!!")

          val canonicalModel: AtomSet = canonicalModels.toArray.apply(index)

          //val sameAreEqual = canonicalModel.asScala.toList.map(atom => isMixed(atom.getTerms().asScala.toList) )

          val expression: Seq[Seq[(Term, Term)]] = canonicalModel.asScala.toList
            .filter(atom => atom.getPredicate.equals(currentAtom.getPredicate)
              && isMixed(atom.getTerms().asScala.toList)
              && currentAtomCompatible(currentAtom, atom))
            .map(atom => getEqualities(currentAtom.getTerms.asScala.toList, atom.getTerms.asScala.toList, List()))

          visitBagAtoms(xs, arrayGeneratingAtoms(index) :: expression :: acc)

        }
    }

    def currentAtomCompatible(currentAtom: Atom, atom: Atom): Boolean = {
      def matches(ct: ConstantType, term: Term): Boolean = {
        if (ct == ConstantType.EPSILON) !ReWriter.isAnonymous(term)
        else ct.identifier._2 == term.toString
      }

      val zipped = currentAtom.getTerms.asScala.toList.zip(atom.getTerms.asScala)
      val mapping = zipped
        .map(f => matches(theType.homomorphism.createImageOf(f._1).asInstanceOf[ConstantType], f._2))

      mapping.reduce(_ && _)
    }

    // makeAtoms
    visitBagAtoms(bag.atoms.toList, List())
  }

  def generateRewriting(borderType: Type, splitter: Splitter): List[RuleTemplate] = {
    val typeExtender =  new TypeExtender(splitter.getSplittingVertex, borderType.homomorphism, canonicalModels.toVector)
    val types = typeExtender.collectTypes
    //val body = new LinkedListAtomSet
    //val rule :Rule = new DefaultRule()
    types.map(s => new RuleTemplate(splitter, borderType, s, generatingAtoms, this))
      .flatMap(ruleTemplate => ruleTemplate :: ruleTemplate.GetAllSubordinateRules)
  }

}