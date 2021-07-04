package uk.ac.bbk.dcs.stypes.utils

import fr.lirmm.graphik.graal.api.core.Atom
import fr.lirmm.graphik.graal.core.DefaultAtom
import uk.ac.bbk.dcs.stypes.Clause

import scala.collection.JavaConverters._
import scala.language.postfixOps

object NdlSubstitution {
  private def bodySubstitution(substitutionClause: Clause, clauseBody: List[Atom]): List[Atom] = {
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

  def idbPredicateSubstitution(ndl: List[Clause], predicateIdentifier: String): List[Clause] = {
    val iDbPredicates = ndl.groupBy(_.head.getPredicate)
    val idbSubstitutionOption = iDbPredicates.find(_._1.getIdentifier.toString == predicateIdentifier)
    if (idbSubstitutionOption.isEmpty || idbSubstitutionOption.get._2.size > 1) {
      throw new RuntimeException(s"Predicate $predicateIdentifier does not exist or it is contained in more then one clauses.")
    } else {
      val substitution: List[Clause] = ndl.filterNot(_.head.getPredicate.getIdentifier.toString == predicateIdentifier)
        .map(clause =>
          Clause(clause.head,
            bodySubstitution(idbSubstitutionOption.get._2.head, clause.body)))
      substitution
    }
  }
}
