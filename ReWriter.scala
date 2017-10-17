package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core._
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.atomset.graph.DefaultInMemoryGraphAtomSet
import fr.lirmm.graphik.graal.forward_chaining.DefaultChase

import scala.annotation.tailrec
import scala.collection.JavaConverters._

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
  def makeGeneratingAtoms(ontology: RuleSet): List[Atom] = {

    @tailrec
    def visitRuleSet(rules: List[Rule], acc: List[Atom]): List[Atom] = rules match {
      case List() => acc
      case x :: xs =>
        if (containsExistentialQuantifier(x)) visitRuleSet(xs, acc ::: x.getBody.asScala.toList)
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

    visitRuleSet(ontology.asScala.toList, List())

  }


  /**
    * Generates canonical models for each of the generating atoms
    *
    * @param ontology a RuleSet
    * @return a List[AtomSet]
    */
  def canonicalModelList(ontology: RuleSet): List[AtomSet] =
    canonicalModelList(ontology, makeGeneratingAtoms(ontology))

  private def canonicalModelList(ontology: RuleSet, generatingAtomList: List[Atom]): List[AtomSet] = {

    @tailrec
    def canonicalModelList(generatingAtomList: List[Atom], acc: List[AtomSet]): List[AtomSet] = generatingAtomList match {
      case List() => acc.reverse
      case x :: xs => canonicalModelList(xs, buildChase(x) :: acc)
    }

    def buildChase(atom: Atom): AtomSet = {
      val store: AtomSet = new DefaultInMemoryGraphAtomSet
      store.add(atom)
      val chase = new DefaultChase(ontology, store)
      chase.execute()

      store
    }


    canonicalModelList(generatingAtomList, List())
  }

  def isAnonymous(x: Term): Boolean =
    x.getLabel.toLowerCase.startsWith("ee")


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
            (new DefaultAtom(DatalogPredicate(s"p${next._2}", atom.getPredicate.getArity), atom.getTerm, next._2 == 1  ), next._1, next._3)
        }

        
        
        
        
        
      @tailrec
      def transformBody(body: Any, acc: (List[Atom], Map[Int, Int], Int)): (List[Any], Map[Int, Int], Int) =
        body match {
          case List() => acc
          case x :: xs => x match {
            case List[List[(Term,Term)]] =>
              transformBody(xs, x :: acc._1, acc._2, acc._3))
            case bodyAtom: Atom =>
              val res = transformAtom(bodyAtom, acc._2, acc._3)
              transformBody(xs, (res._1 :: acc._1, res._2, res._3))
            //case _ => OpenUpBrackets()
          }
        }

      val head = transformAtom(ruleTemplate.head, map, currentIndex)
      val bodyWithTransformedAtomsButWithListListPairForEqualities: (List[Any], Map[Int, Int], Int) = transformBody(ruleTemplate.body, (List(), map, currentIndex))
       
      
      val clauses:List[Clause] = OpenUpBrackets(bodyWithTransformedAtomsButWithListListPairForEqualities._1).map(b => Clause(head._1, b))

      (clauses, bodyWithTransformedAtomsButWithListListPairForEqualities._2, bodyWithTransformedAtomsButWithListListPairForEqualities._3)
    }

    def EqualityAtom((x:Term, y:Term)):Atom = {
        Equality(x, y).getAtom
    }
    
    def EqualityAtomConjunction(list:List[(Term,Term)]):List[Atom] = {
        list.map(equality => EqualityAtom(equality))
    }
    
    def OpenUpBrackets(body:List[Any]) : List[List[Atom]] = {
      body match {
        case List() => List()
        case head::tail => head match {
          case x:Atom=> OpenUpBrackets(tail).map(t => x::t)
          case x:List[List[(Term, Term)]] => {
              x.flatMap( equalityConj => OpenUpBrackets(tail).map (t => EqualityAtomConjunction(equalityConj)::t))
          }
        }
      }
    }

    def visitRewriting(rewriting: List[RuleTemplate], acc: (List[List[Clause]], Map[Int, Int], Int)): (List[List[Clause]], Map[Int, Int], Int) =
      rewriting match {
        case List() => acc
        case x :: xs =>
          val res = getAtomsFromRewrite(x, acc._2, acc._3)
          visitRewriting(xs, (res._1 :: acc._1, res._2, res._3))
      }

    val datalog = visitRewriting(rewriting.toList, (List(), Map(), 0))._1.flatMap(_).reverse


    def removeEmptyClauses(datalog: List[Clause]): List[Clause] = {

      def removalHelper(datalog: List[Clause]): (List[Clause], Boolean) = {
        val defined = datalog.map(_.head.getPredicate).toSet

        def remove(l: List[Atom]): Boolean = l match {
          case List() => false
          case x :: xs => x.getPredicate match {
            case p: DatalogPredicate =>
              !defined.contains(x.getPredicate) || remove(xs)
            case _ =>
              remove(xs)
          }
        }

        val ret = datalog.filter(rule => !remove(rule.body))
        (ret, ret.size != datalog.size)
      }

      val removalOutcome = removalHelper(datalog)

      val iterate = removalOutcome._2

      val removalResult = removalOutcome._1

      if (iterate)
        removalHelper(removalResult)._1
      else
        removalResult
    }

    def removeDuplicate (datalog: List[Clause]): List[Clause] = datalog.toSet.toList

    def predicateSubstitution(datalog: List[Clause]): List[Clause] = {

      val toBeSubstituted = datalog
        .sortBy(_.head.getPredicate)
        .map(p => (p.head.getPredicate, 1L))
        .groupBy(_._1)
        .filter(_._2.size == 1)
        .keys.toList

      val substitutionTable: Map[Atom, List[Atom]] = datalog
        .filter(p => toBeSubstituted.contains(p.head.getPredicate))
        .map(p => p.head -> p.body).toMap

      def visitPredicates(datalog: List[Atom], acc: (List[Atom], Boolean)): (List[Atom], Boolean) = datalog match {
        case List() => (acc._1.reverse, acc._2)
        case atom :: xs => atom.getPredicate match {
          case p: DatalogPredicate =>
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

    val removalResult: List[Clause] = removeEmptyClauses( removeDuplicate( datalog ))

    predicateSubstitution(removalResult)

  }


}


class ReWriter(ontology: RuleSet) {

  val generatingAtoms: List[Atom] = ReWriter.makeGeneratingAtoms(ontology)
  val canonicalModels: List[AtomSet] = ReWriter.canonicalModelList(ontology, generatingAtoms)
  private val arrayGeneratingAtoms = generatingAtoms.toVector

  /**
    * Given a type t defined on a bag, it computes the formula At(t)
    *
    * @param bag     a Bag
    * @param theType a Type
    * @return a List[Atom]
    */
  def makeAtoms(bag: Bag, theType: Type): List[Any] = {


    def getEqualities(variables: List[Term], constants: List[Term], acc: List[(Term, Term)]): List[(Term, Term)] = constants match {
      case List() => acc
      case x :: xs =>
        if (ReWriter.isAnonymous(x)) getEqualities(variables.tail, xs, acc)
        else getEqualities(variables.tail, xs, (variables.head, x) :: acc)

    }


    def isMixed(terms: List[Term]): Boolean =
      atLeastOneTerm(terms, x => ReWriter.isAnonymous(x)) && atLeastOneTerm(terms, x => !ReWriter.isAnonymous(x))

    def atLeastOneTerm(terms: List[Term], f: Term => Boolean): Boolean = terms match {
      case List() => false
      case x :: xs => if (f(x)) true else atLeastOneTerm(xs, f)
    }


    @tailrec
    def visitBagAtoms(atoms: List[Atom], acc: List[Any]): List[Any] = atoms match {
      case List() => acc.reverse
      case currentAtom :: xs =>
        // All epsilon
        if (theType.areAllEpsilon(currentAtom)) visitBagAtoms(xs, currentAtom :: acc)
        // All anonymous
        else if (theType.areAllAnonymous(currentAtom)) {
          theType.homomorphism.createImageOf(currentAtom.getTerm(0)) match {
            case constantType: ConstantType =>
              val index = constantType.identifier._1
              if (index < arrayGeneratingAtoms.length) {
                val atom = arrayGeneratingAtoms(index)
                visitBagAtoms(xs, atom :: acc)
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

          val expression: List[List[(Term, Term)]] = canonicalModel.asScala.toList
            .filter(atom => atom.getPredicate.equals(currentAtom.getPredicate) && isMixed(atom.getTerms().asScala.toList))
            .map(atom => getEqualities(currentAtom.getTerms.asScala.toList, atom.getTerms.asScala.toList, List()))


          visitBagAtoms(xs, arrayGeneratingAtoms(index) :: expression :: acc)

        }
    }

    // makeAtoms
    visitBagAtoms(bag.atoms.toList, List())
  }

  def generateRewriting(borderType: Type, splitter: Splitter): List[RuleTemplate] = {
    val typeExtender = new TypeExtender(splitter.getSplittingVertex, borderType.homomorphism, canonicalModels.toVector)
    val types = typeExtender.collectTypes
    //val body = new LinkedListAtomSet
    //val rule :Rule = new DefaultRule()
    types.map(s => new RuleTemplate(splitter, borderType, s, generatingAtoms, this)).flatMap(ruleTemplate => ruleTemplate :: ruleTemplate.GetAllSubordinateRules)
  }

}