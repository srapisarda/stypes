package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Rule, RuleSet, Term}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
/**
  * Created by
  *   Salvatore Rapisarda
  *   Stanislav Kikot
  *
  * on 02/05/2017.
  */
object ReWriter {

  def generateAtoms(ruleSet:RuleSet):List[Atom] = {

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

    visitRuleSet(ruleSet.asScala.toList, List())

  }

}
