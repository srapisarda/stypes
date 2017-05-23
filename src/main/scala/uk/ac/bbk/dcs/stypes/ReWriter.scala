package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core._
import fr.lirmm.graphik.graal.forward_chaining.DefaultChase
import fr.lirmm.graphik.graal.core.atomset.graph.DefaultInMemoryGraphAtomSet

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
      case List() => acc
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


  /**
    * Given a type t defined on a bag, it computes the formula At(t)
    *
    * @param bag a Bag
    * @param t   a Type
    * @return a List[Atom]
    */
  def makeAtoms(bag: Bag, t: Type): List[Atom] = {
    @tailrec
    def visitBagAtoms(atoms: List[Atom], acc: List[Atom]): List[Atom] = atoms match {
      case List() => acc.reverse
      case x :: xs =>
        if (t.areAllEpsilon(x)) visitBagAtoms(xs, x :: acc)
        else if (t.areAllAnonymous(x)) {
          val atom: Option[Atom] = t.genAtoms.get(x.getTerm(0))
          if (atom.isDefined) visitBagAtoms(xs, atom.get :: acc)
          else visitBagAtoms(xs, acc)
        }
        else visitBagAtoms(xs, acc)

    }

    visitBagAtoms(bag.atoms.toList, List())

  }


}
