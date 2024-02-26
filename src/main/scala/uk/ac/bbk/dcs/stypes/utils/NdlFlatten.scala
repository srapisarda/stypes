package uk.ac.bbk.dcs.stypes.utils

import com.typesafe.scalalogging.Logger
import fr.lirmm.graphik.graal.api.core.{Atom, Term}
import fr.lirmm.graphik.graal.core.DefaultAtom
import uk.ac.bbk.dcs.stypes.{Clause, Equality, ReWriter}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.language.postfixOps

object NdlFlatten {
  private val logger = Logger(this.getClass)

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

      def getEqualitySubstitutionMap(substitutionList: List[(Term, Term)]) =
        substitutionList
          .groupBy(_._1)
          .filter(_._2.size > 1)
          .flatten{case (_, value ) => {
            val last =value.last._2
            value
              .filterNot( _._2 == last)
              .map( v => (  v._2, last ))
          }}.toMap

      def applySubstitution(sigma: Map[Term, Term], atom: Atom, clause: Clause) = {
        sigma.foreach { case (t1, t2) =>
            clause.body.filter(_.contains(t1))
              .foreach(atom => atom.setTerm(atom.indexOf(t1), t2))
            if (clause.head.contains(t1)) {
              clause.head.setTerm(atom.indexOf(t1), t2)
            }
          }
        clause
      }

      def applySubstitutionToList(sigma: Map[Term, Term], atom: Atom, body: List[Atom]) = {
        sigma.foreach{ case (t2, t1) =>
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
            logger.debug(s"   substitutionList: $substitutionList")
            val substitutionCloseBody = getSubstitutionCloseBody(substitutionList.toMap)
            logger.debug(s"   substitutionCloseBody: $substitutionCloseBody")
            val newClause = Clause(clauseResult.head, clauseResult.body ::: substitutionCloseBody)
            logger.debug(s"   newClause: $newClause")
            val sigma = getEqualitySubstitutionMap(substitutionList)
            logger.debug(s"   sigma: $sigma")
            val equalitySubstitutionTail = applySubstitutionToList(sigma, atom, tail)
            logger.debug(s"   tail: $equalitySubstitutionTail")
            val equalitySubstitutionResult = applySubstitution(sigma, atom, newClause)
            logger.debug(s"   newResult: $equalitySubstitutionResult")
            traverseClause(equalitySubstitutionTail, equalitySubstitutionResult)
          } else {
            logger.debug(s"   no-substitution ")
            logger.debug(s"   tail: $tail")
            val newClauseResult = Clause(clauseResult.head, clauseResult.body :+ new DefaultAtom(atom))
            logger.debug(s"   newClauseResult: $newClauseResult")
            traverseClause(tail, newClauseResult )
          }
      }

      val substituted = traverseClause(clause.body, Clause(new DefaultAtom(clause.head), List()))
      Clause(substituted.head, substituted.body.distinct )
    }

    substitutionClauses.map(clauseSubstitutionH)
  }

  def idbPredicateFlatten(ndl: List[Clause], predicateIdentifier: String): List[Clause] = {
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
