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


  def generateDatalog(rewriting: Seq[RuleTemplate]): Set[Clause] = {

    def getAtomsFromRewrite(ruleTemplate: RuleTemplate, map: Map[Int, Int], currentIndex: Int): (Clause, Map[Int, Int], Int) = {

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
            (new DefaultAtom(DatalogPredicate(s"p${next._2}", atom.getPredicate.getArity), atom.getTerms), next._1, next._3)
        }

      @tailrec
      def transformBody(body: Any, acc: (List[Atom], Map[Int, Int], Int)): (List[Atom], Map[Int, Int], Int) =
        body match {
          case List() => acc
          case x :: xs => x match {
            case List(List((x: Term, y: Term))) =>
              transformBody(xs, (Equality(x, y).getAtom :: acc._1, acc._2, acc._3))
            case bodyAtom: Atom =>
              val res = transformAtom(bodyAtom, acc._2, acc._3)
              transformBody(xs, (res._1 :: acc._1, res._2, res._3))
          }
        }

      val head = transformAtom(ruleTemplate.head, map, currentIndex)
      val body: (List[Atom], Map[Int, Int], Int) = transformBody(ruleTemplate.body, (List(), head._2, head._3))

      val clause = Clause(head._1, body._1.reverse)

      (clause, body._2, body._3)
    }

    def visitRewriting(rewriting: List[RuleTemplate], acc: (List[Clause], Map[Int, Int], Int)): (List[Clause], Map[Int, Int], Int) =
      rewriting match {
        case List() => acc
        case x :: xs =>
          val res = getAtomsFromRewrite(x, acc._2, acc._3)
          visitRewriting(xs, (res._1 :: acc._1, res._2, res._3))
      }

    val datalog = visitRewriting(rewriting.toList, (List(), Map(), 0))._1.reverse.toSet

    val toBeRemoved1 = datalog.toList.map(p => (p.head.getPredicate, 1L)).groupBy(_._1)

    val toBeRemoved = toBeRemoved1.filter(_._2.size == 1).keys.toList

    def predicateSubstitution(datalog: Set[Clause]): Set[Clause] = {

      val toSubstitute: Map[Atom, List[Atom]] = datalog
        .filter(p => toBeRemoved.contains(p.head.getPredicate))
        .map(p => p.head -> p.body ).toMap

      def visitPredicates(datalog: List[Atom], acc: (List[Atom], Boolean) ): (List[Atom], Boolean) = datalog match {
        case List() =>  (acc._1.reverse, acc._2)
        case atom::xs => atom.getPredicate match {
          case p:DatalogPredicate =>
            //println(s"datalog predicate $p")
            if ( toSubstitute.contains(atom) )
              visitPredicates( xs, (toSubstitute(atom) ::: acc._1, true)  )
            else
              visitPredicates( xs, (atom :: acc._1, acc._2) )
          case p:Any =>
            //println(s"data predicate $p")
            visitPredicates( xs, (atom :: acc._1, acc._2) )
        }
      }

      def substitution(datalog: List[Clause]) :List[Clause] = {

        def substitutionH(datalog: List[Clause]): List[(Clause, Boolean)] = {
          datalog.map(d => {
            val visit = visitPredicates(d.body, (List(), false))
            (Clause(d.head, visit._1), visit._2)
          })
        }

        val sub = substitutionH(datalog)

        val hasSubstitution: Boolean = sub.map(_._2).reduce((s1, s2)  => s1 || s2 )

        if ( hasSubstitution )
          substitution(sub.map(_._1))
        else
          sub.map(_._1)
      }

      val sub = substitution(datalog.toList).filter( p=>  ! toBeRemoved.contains( p.head.getPredicate )  )

      println( sub.mkString(".\n") )
      println("-----")

      datalog

    }

    predicateSubstitution( datalog  )
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
