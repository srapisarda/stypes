package uk.ac.bbk.dcs.stypes.utils

import fr.lirmm.graphik.graal.api.core.Atom
import fr.lirmm.graphik.graal.core.DefaultAtom
import uk.ac.bbk.dcs.stypes.Clause

import scala.collection.JavaConverters._
import scala.language.postfixOps

object NdlSubstitution {
  private def bodySubstitutions(substitutionClauses: List[Clause], clauseBody: List[Atom]): List[List[Atom]] = {

    def bodySubstitutionH(substitutionClause: Clause) = {
      val zipTermsWithIndexSubstitution = ((Stream from 0) zip substitutionClause.head.getTerms.asScala) toMap

      val bodySubstituted: List[Atom] = clauseBody
        .flatten(atom => {
          if (atom.getPredicate == substitutionClause.head.getPredicate) {
            val substitutionMap = atom.getTerms.asScala
              .zipWithIndex
              .map(termZipped => (zipTermsWithIndexSubstitution(termZipped._2), termZipped._1)).toMap

            substitutionClause.body.map(atomToSubstitute => {
              val newAtom = new DefaultAtom(atomToSubstitute)
              atomToSubstitute.getTerms.asScala.zipWithIndex.foreach(termIndexed => {
                if (substitutionMap.contains(termIndexed._1)) {
                  newAtom.setTerm(termIndexed._2, substitutionMap(termIndexed._1))
                }
              })
              newAtom
            })
          } else {
            List(atom)
          }
        })

      bodySubstituted.distinct
    }

    substitutionClauses.map(bodySubstitutionH)
  }

  def idbPredicateSubstitution(ndl: List[Clause], predicateIdentifier: String): List[Clause] = {
    val iDbPredicates = ndl.groupBy(_.head.getPredicate)
    val idbSubstitutionOption = iDbPredicates.find(_._1.getIdentifier.toString == predicateIdentifier)
    if (idbSubstitutionOption.nonEmpty) {
      val substitutionClauses = idbSubstitutionOption.get._2
      ndl.filterNot(_.head.getPredicate.getIdentifier.toString == predicateIdentifier)
        .flatten(clause => {
          bodySubstitutions(substitutionClauses, clause.body)
            .map(bodySubstituted => Clause(clause.head, bodySubstituted))
        })
    }
    else ndl
  }
}
