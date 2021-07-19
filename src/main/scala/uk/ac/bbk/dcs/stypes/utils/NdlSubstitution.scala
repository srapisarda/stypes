package uk.ac.bbk.dcs.stypes.utils

import fr.lirmm.graphik.graal.api.core.Atom
import fr.lirmm.graphik.graal.core.DefaultAtom
import uk.ac.bbk.dcs.stypes.Clause

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.language.postfixOps

object NdlSubstitution {
  private def clauseSubstitution(substitutionClauses: List[Clause], clause: Clause) = {

    def clauseSubstitutionH(substitutionClause: Clause) = {
      val zipTermsWithIndexSubstitution = ((Stream from 0) zip substitutionClause.head.getTerms.asScala) toMap

      @tailrec
      def traverseClause(clauseBody: List[Atom], clauseResult: Clause): Clause = clauseBody match {
        case Nil =>
          clauseResult

        case atom :: tail =>
          if (atom.getPredicate == substitutionClause.head.getPredicate) {
            val substitutionList = atom.getTerms.asScala
              .zipWithIndex
              .map(termZipped => (zipTermsWithIndexSubstitution(termZipped._2), termZipped._1))

            val substitutionMap = substitutionList.toMap

            val res = substitutionClause.body.map(atomToSubstitute => {
              val newAtom = new DefaultAtom(atomToSubstitute)
              atomToSubstitute.getTerms.asScala.zipWithIndex.foreach(termIndexed => {
                if (substitutionMap.contains(termIndexed._1)) {
                  newAtom.setTerm(termIndexed._2, substitutionMap(termIndexed._1))
                }
              })
              newAtom
            })
            // todo: check on the clause head for any substitution.
            traverseClause(tail, Clause(clauseResult.head, clauseResult.body ::: res))
          } else {
            traverseClause(tail, Clause(clauseResult.head, clauseResult.body :+ atom))
          }

      }

      val substituted = traverseClause(clause.body, Clause(clause.head, List()))
      Clause(substituted.head, substituted.body distinct)
    }

    substitutionClauses.map(clauseSubstitutionH)
  }

  def idbPredicateSubstitution(ndl: List[Clause], predicateIdentifier: String): List[Clause] = {
    val iDbPredicates = ndl.groupBy(_.head.getPredicate)
    val idbSubstitutionOption = iDbPredicates.find(_._1.getIdentifier.toString == predicateIdentifier)
    if (idbSubstitutionOption.nonEmpty) {
      val substitutionClauses = idbSubstitutionOption.get._2
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
}
