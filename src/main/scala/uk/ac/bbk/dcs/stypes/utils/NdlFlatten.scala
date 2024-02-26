package uk.ac.bbk.dcs.stypes.utils

import com.typesafe.scalalogging.Logger
import fr.lirmm.graphik.graal.api.core.{Atom, Term}
import fr.lirmm.graphik.graal.core.DefaultAtom
import uk.ac.bbk.dcs.stypes.{Clause, Equality, ReWriter}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.language.postfixOps

object NdlFlatten {
  val logger = Logger(this.getClass)
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

//
//        substitutionList.groupBy(_._1).filter(_._2.length > 1).flatten(g => g._2.map( p =>  Equality(p._1, p._2))).toList
      def getEqualitySubstitution(substitutionList: List[(Term, Term)]) =
        substitutionList
          .groupBy(_._1)
          .filter(_._2.length > 1)
          .flatten(g => {
            g._2.sliding(2)
              .toList
              .map(list =>
                Equality(list.tail.head._2, list.head._2))
          }).toList

      def applyEqualitySubstitution(equalities: List[Equality], atom: Atom, clause: Clause) = {
        equalities.foreach(
          equality => {
            clause.body.filter(_.contains(equality.t2))
              .foreach(atom => atom.setTerm(atom.indexOf(equality.t2), equality.t1))
            if (clause.head.contains(equality.t2)) {
              clause.head.setTerm(atom.indexOf(equality.t2), equality.t1)
            }
          })
        clause
      }

      def applyEqualitySubstitutionToBody(equalities: List[Equality], atom: Atom, body: List[Atom]) = {
        equalities.foreach(
          equality => {
            body.filter(_.contains(equality.t2))
              .foreach(atom => atom.setTerm(atom.indexOf(equality.t2), equality.t1))
          })
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
            val equalitySubstitution = getEqualitySubstitution(substitutionList)
            logger.debug(s"   equalitySubstitution: $equalitySubstitution")
            val equalitySubstitutionTail = applyEqualitySubstitutionToBody(equalitySubstitution, atom, tail)
            logger.debug(s"   tail: $equalitySubstitutionTail")
            val equalitySubstitutionResult = applyEqualitySubstitution(equalitySubstitution, atom, newClause)
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
