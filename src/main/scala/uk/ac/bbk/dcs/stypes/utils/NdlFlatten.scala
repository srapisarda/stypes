package uk.ac.bbk.dcs.stypes.utils

import com.typesafe.scalalogging.Logger
import fr.lirmm.graphik.graal.api.core.{Atom, Term}
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import fr.lirmm.graphik.graal.core.DefaultAtom
import uk.ac.bbk.dcs.stypes.{Clause, ReWriter, SplitClause}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.language.postfixOps


case class Flatten(head: Atom, processed: List[Atom] = List())

object NdlFlatten {
  private val logger = Logger(this.getClass)

  def copyFromAtom(atom: Atom, index: Int): Atom = {
    val freshAtom = new DefaultAtom(atom)
    freshAtom.getTerms.asScala.indices foreach (i => {
      freshAtom.setTerm(i,
        DefaultTermFactory.instance().createTerm(s"${freshAtom.getTerm(i).getIdentifier.toString}_$index", freshAtom.getTerm(i).getType)
      )
    })
    freshAtom
  }

  private def copyXi(xi: List[Clause], index: Int): List[Clause] = {
    xi.map(clause => {
      val head: Atom = copyFromAtom(clause.head, index)
      val body = clause.body.map(atom => copyFromAtom(atom, index))
      Clause(head, body)
    })
  }

  private def getAtomTermsZipped(z1: Atom, y: Atom): List[(Term, Term)] = {
    // a list of pairs of the form (z′(i), y(i))
    0 until z1.getTerms.size() map (i => (z1.getTerm(i), y.getTerm(i))) toList
  }

  @tailrec
  private def getMGUForGroup(sigma: Map[Term, Term], list: List[(Term, Term)], yk: Term): Map[Term, Term] = list match {
    case List() => sigma
    case (_, y) :: tail => getMGUForGroup(sigma + (y -> yk), tail, yk)
  }

  private def getMgu(list: List[(Term, Term)]): Map[Term, Term] = {
    list.groupBy(_._1)
      .flatten {
        case (z1, list) =>
          val lastTerm = list.last._2
          getMGUForGroup(Map(z1 -> lastTerm), list, lastTerm)
      }.toMap
  }

  private def substitute(sigma: Map[Term, Term], body: List[Atom]): List[Atom] = {
    body.map(atom => {
      val newAtom = new DefaultAtom(atom)
      atom.getTerms.asScala.zipWithIndex.foreach(termIndexed => {
        if (sigma.contains(termIndexed._1)) {
          newAtom.setTerm(termIndexed._2, sigma(termIndexed._1))
        }
      })
      newAtom
    })
  }

  private def clauseSubstitution2(xi: List[Clause], clause: Clause): List[Clause] = {

    val predicateToSubstitute = xi.head.head.getPredicate.getIdentifier

    @tailrec
    def getSubstitutionsH(F: List[SplitClause], index: Int): List[SplitClause] = {
      if (F.isEmpty || F.head.unprocessedBody.isEmpty) {
        F
      } else {
        val F1 =
          for (f <- F) yield {
            val atomToSubstitute = f.unprocessedBody.head
            if (atomToSubstitute.getPredicate.getIdentifier == predicateToSubstitute) {
              val xiC: List[Clause] = copyXi(xi, index)
              val ff = for (xiClause <- xiC) yield {
                val termsZipped = getAtomTermsZipped(xiClause.head, atomToSubstitute)
                val sigma = getMgu(termsZipped)
                val sigmaBody = substitute(sigma, f.processedBody ++ xiClause.body)
                val sigmaHead = substitute(sigma, List(f.head)).head
                SplitClause(sigmaHead, sigmaBody, substitute(sigma, f.unprocessedBody.tail))
              }
              ff
            } else {
              List(SplitClause(f.head, f.processedBody :+ atomToSubstitute, f.unprocessedBody.tail))
            }
          }
        getSubstitutionsH(F1.flatten, index + 1)
      }
    }

    val res = getSubstitutionsH(List(SplitClause(clause.head, List(), clause.body)), 0)
    res.map(_.toClause)
  }

  private def clauseSubstitution(substitutionClauses: List[Clause], clause: Clause) = {

    def clauseSubstitutionH(substitutionClause: Clause) = {
      val zipTermsWithIndexSubstitution = ((Stream from 0) zip substitutionClause.head.getTerms.asScala) toMap

      def getSubstitutionList(atom: Atom) = atom.getTerms.asScala
        .zipWithIndex
        .map(termZipped => (zipTermsWithIndexSubstitution(termZipped._2), termZipped._1))
        .toList

      def getSubstitutionCloseBody(substitutionMap: Map[Term, Term]) =
        substitutionClause.body.map(atomToSubstitute => {
          val newAtom = new DefaultAtom(atomToSubstitute)
          atomToSubstitute.getTerms.asScala.zipWithIndex.foreach(termIndexed => {
            if (substitutionMap.contains(termIndexed._1)) {
              newAtom.setTerm(termIndexed._2, substitutionMap(termIndexed._1))
            }
          })
          newAtom
        })

      def getMGU(substitutionList: List[(Term, Term)]) =
        substitutionList
          .groupBy(_._1)
          .filter(_._2.size > 1)
          .flatten {
            case (_, value) =>
              val last = value.last._2
              value
                .filterNot(_._2 == last)
                .map(v => (v._2, last))
          }.toMap

      def applySubstitution(sigma: Map[Term, Term], atom: Atom, clause: Clause) = {
        sigma.foreach {
          case (t1, t2) =>
            clause.body.filter(_.contains(t1))
              .foreach(atom => atom.setTerm(atom.indexOf(t1), t2))
            if (clause.head.contains(t1)) {
              clause.head.setTerm(atom.indexOf(t1), t2)
            }
        }
        clause
      }

      def applySubstitutionToList(sigma: Map[Term, Term], body: List[Atom]) = {
        sigma.foreach {
          case (t2, t1) =>
            body.filter(_.contains(t2))
              .foreach(atom => atom.setTerm(atom.indexOf(t2), t1))
        }
        body
      }

      @tailrec
      def traverseClause(clauseBody: List[Atom], clauseResult: Clause): Clause = clauseBody match {
        case Nil =>
          clauseResult

        case atom :: tail =>
          if (atom.getPredicate == substitutionClause.head.getPredicate) {
            logger.debug(s"   substitution ")
            logger.debug(s"   atom: $atom")
            val substitutionList = getSubstitutionList(atom)
            val sigma = getMGU(substitutionList)
            logger.debug(s"   substitutionList: $substitutionList")
            val substitutionCloseBody = getSubstitutionCloseBody(substitutionList.toMap)
            logger.debug(s"   substitutionCloseBody: $substitutionCloseBody")
            val newClause = Clause(clauseResult.head, clauseResult.body ::: substitutionCloseBody)
            logger.debug(s"   newClause: $newClause")
            logger.debug(s"   sigma: $sigma")
            val equalitySubstitutionTail = applySubstitutionToList(sigma, tail)
            logger.debug(s"   tail: $equalitySubstitutionTail")
            val equalitySubstitutionResult = applySubstitution(sigma, atom, newClause)
            logger.debug(s"   newResult: $equalitySubstitutionResult")
            traverseClause(equalitySubstitutionTail, equalitySubstitutionResult)
          } else {
            logger.debug(s"   no-substitution ")
            logger.debug(s"   tail: $tail")
            val newClauseResult = Clause(clauseResult.head, clauseResult.body :+ new DefaultAtom(atom))
            logger.debug(s"   newClauseResult: $newClauseResult")
            traverseClause(tail, newClauseResult)
          }
      }

      val substituted = traverseClause(clause.body, Clause(new DefaultAtom(clause.head), List()))
      Clause(substituted.head, substituted.body.distinct)
    }

    substitutionClauses.map(clauseSubstitutionH)
  }

  def idbPredicateFlatten(ndl: List[Clause], predicateIdentifier: String): List[Clause] = {
    val iDbPredicates = ndl.groupBy(_.head.getPredicate)
    val idbSubstitutionOption = iDbPredicates.find(_._1.getIdentifier.toString == predicateIdentifier)
    logger.debug(s"idbSubstitutionOption: $idbSubstitutionOption")
    if (idbSubstitutionOption.isEmpty) ndl
    else {
      val substitutionClauses = idbSubstitutionOption.get._2
      logger.debug(s"substitutionClauses: $substitutionClauses")
      val clauseToCheck = ndl.filterNot(_.head.getPredicate.getIdentifier.toString == predicateIdentifier)
      val fi = for (clause <- clauseToCheck) yield {
        if (clause.body.exists(_.getPredicate.getIdentifier.toString == predicateIdentifier)) {
          val res = clauseSubstitution2(substitutionClauses, clause)
          res
        } else {
          List(clause)
        }
      }
      fi.flatten
    }
  }

  @deprecated
  def idbPredicateFlattenOld(ndl: List[Clause], predicateIdentifier: String): List[Clause] = {
    val iDbPredicates = ndl.groupBy(_.head.getPredicate)
    val idbSubstitutionOption = iDbPredicates.find(_._1.getIdentifier.toString == predicateIdentifier)
    logger.debug(s"idbSubstitutionOption: $idbSubstitutionOption")
    if (idbSubstitutionOption.nonEmpty) {
      val substitutionClauses = idbSubstitutionOption.get._2
      logger.debug(s"substitutionClauses: $substitutionClauses")
      ndl.filterNot(_.head.getPredicate.getIdentifier.toString == predicateIdentifier)
        .flatten(clause => {
          if (clause.body.exists(_.getPredicate.getIdentifier.toString == predicateIdentifier)) {
            clauseSubstitution(substitutionClauses, clause)
          } else {
            List(clause)
          }
        })
    }
    else ndl
  }

  def main(args: Array[String]): Unit = {
    if (args.isEmpty || args.length < 2) println("Please provide the path to NDL file and predicate to substitute")
    else {
      val ndl = ReWriter.getDatalogRewriting(args(0))
      val substitution = idbPredicateFlatten(ndl, args(1))
      println(substitution.mkString("\n"))
    }
  }
}
