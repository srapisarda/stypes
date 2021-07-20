package uk.ac.bbk.dcs.stypes.utils

import fr.lirmm.graphik.graal.api.core.Atom
import fr.lirmm.graphik.graal.core.DefaultAtom
import uk.ac.bbk.dcs.stypes.{Clause, Equality}

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
              .toList

            val substitutionMap = substitutionList.toMap

            val res : List[Atom] = substitutionClause.body.map(atomToSubstitute => {
              val newAtom = new DefaultAtom(atomToSubstitute)
              atomToSubstitute.getTerms.asScala.zipWithIndex.foreach(termIndexed => {
                if (substitutionMap.contains(termIndexed._1)) {
                  newAtom.setTerm(termIndexed._2, substitutionMap(termIndexed._1))
                }
              })
              newAtom
            })

            val equalities = substitutionList.groupBy(_._1).filter(_._2.length > 1).flatten(g => {
              g._2.sliding(2)
                .toList
                .map(list => Equality(list.head._1, list.head._2))
            }).toList

            val newBody = clauseResult.body ::: res ::: equalities

//            equalities.foreach(
//              equality => {
//                newBody.filter(_.contains(equality.t2))
//                  .foreach(atom => atom.setTerm(atom.indexOf(equality.t2), equality.t1))
//                if (clauseResult.head.contains(equality.t2)) {
//                  clauseResult.head.setTerm(atom.indexOf(equality.t2), equality.t1)
//                }
//              })

            //            substitutionList
            //              .filterNot(tuple => tuple._1 == tuple._2).foreach {
            //              case (sigma1, sigma2) => {
            //                // body substitution
            //                newBody.filter(_.contains(sigma2))
            //                  .foreach(atom => atom.setTerm(atom.indexOf(sigma2), sigma1))
            //                // head substitution
            //                if (clauseResult.head.contains(sigma2)) {
            //                  res.head.setTerm(atom.indexOf(sigma2), sigma1)
            //                }
            //              }
            //            }

            // todo: check on the clause head for any substitution.
            traverseClause(tail, Clause(clauseResult.head, newBody))
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
