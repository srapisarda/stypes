package uk.ac.bbk.dcs.stypes.optimise

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.core.DefaultAtom
import uk.ac.bbk.dcs.stypes.Clause

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object SubExpressionSubstitution extends OptimisationRule {
  val joinSharedPredicateIdentifier = "comp_"

  override def optimise(datalog: List[Clause], statistics: CatalogStatistics): List[Clause] = {

    val joinTuplesMapToClauses: Map[JoinTuple, List[Clause]] =
      getJoinTuplesMapToClauses(datalog).filter { case (_, clauses) => clauses.length > 1 }

    if (joinTuplesMapToClauses.nonEmpty) {
      var dataLogUpdated = datalog
      joinTuplesMapToClauses.zipWithIndex.foreach {
        case ((joinTuples, clausesToUpdate), index) =>
          dataLogUpdated = updateDlpByAddingNewJoinSharedAtom(datalog,
            joinTuples,
            clausesToUpdate,
            joinTuples.getJoinSharedClause(joinSharedPredicateIdentifier + index))
      }
      dataLogUpdated
    } else {
      datalog
    }
  }

  def updateDlpByAddingNewJoinSharedAtom(datalog: List[Clause],
                                         joinTuples: JoinTuple,
                                         clausesToUpdate: List[Clause],
                                         joinSharedClause: Clause): List[Clause] = clausesToUpdate match {
    case Nil =>
      joinSharedClause :: datalog

    case clauseToUpdate :: tail =>
      val atomsToSubstitute = clauseToUpdate
        .body
        .filter(atom => atom.getPredicate == joinTuples.atomA.getPredicate &&
          atom.indexOf(joinTuples.term) == joinTuples.positionTermA ||
          (atom.getPredicate == joinTuples.atomB.getPredicate &&
            atom.indexOf(joinTuples.term) == joinTuples.positionTermB))

      if (atomsToSubstitute.size == joinSharedClause.body.size) {
        val updatedBody: List[Atom] =
          JoinTuple(atomsToSubstitute.head,
            atomsToSubstitute.tail.head,
            joinTuples.positionTermA,
            joinTuples.positionTermB,
            atomsToSubstitute.head.getTerm(joinTuples.positionTermA))
            .getJoinSharedClause(joinSharedClause.head.getPredicate.getIdentifier.toString).head ::
            clauseToUpdate.body.filter(!atomsToSubstitute.contains(_))

        val updatedClause: Clause = Clause(clauseToUpdate.head, updatedBody)
        val updatedDatalog: List[Clause] = updatedClause :: datalog.filter(_ != clauseToUpdate)
        updateDlpByAddingNewJoinSharedAtom(updatedDatalog, joinTuples, tail, joinSharedClause)
      } else {
        updateDlpByAddingNewJoinSharedAtom(datalog, joinTuples, tail, joinSharedClause)
      }
  }

  private def filterJoinTuple(joinTuples: List[JoinTuple], acc: List[JoinTuple] = Nil): List[JoinTuple] = joinTuples
  match {
    case Nil => acc

    case joinTuple :: tail =>
      if (acc.exists(jt => jt.atomB == joinTuple.atomA && jt.atomA == joinTuple.atomB &&
        jt.positionTermA == joinTuple.positionTermB && jt.positionTermB == joinTuple.positionTermA)) {
        filterJoinTuple(tail, acc)
      }
      else {
        filterJoinTuple(tail, joinTuple :: acc)
      }
  }

  @tailrec
  private def getJoinTuplesMapToClauses(datalog: List[Clause],
                                        map: Map[JoinTuple, List[Clause]] =
                                        Map()): Map[JoinTuple, List[Clause]] = datalog match {
    case Nil => map

    case clause :: tail =>
      val joinTuples: List[JoinTuple] = filterJoinTuple(clause.body.flatten(atom => {
        atom.getTerms.asScala.toList
          .flatten(term =>
            clause.body
              .filter(_ != atom)
              .find(_.getTerms.asScala.contains(term)).toList
              .map(bodyAtom => JoinTuple(atom, bodyAtom, atom.indexOf(term), bodyAtom.indexOf(term), term)))
      }))

      val updatedMap: Map[JoinTuple, List[Clause]] = joinTuples.map(jt => {
        if (map.contains(jt)) {
          (jt, clause :: map(jt))
        } else {
          (jt, clause :: Nil)
        }
      }).toMap

      getJoinTuplesMapToClauses(tail, updatedMap)
  }


  case class JoinTuple(atomA: Atom, atomB: Atom, positionTermA: Int, positionTermB: Int, term: Term) {

    override def equals(obj: Any): Boolean = {
      if (obj == null) {
        false
      }
      else if (!obj.isInstanceOf[JoinTuple]) {
        false
      }
      else {
        val that = obj.asInstanceOf[JoinTuple]
        this.atomA.getPredicate.getIdentifier == that.atomA.getPredicate.getIdentifier &&
          this.atomB.getPredicate.getIdentifier == that.atomB.getPredicate.getIdentifier
        this.positionTermA == that.positionTermA && this.positionTermB == that.positionTermB
      }
    }

    def getJoinSharedClause(predicateIdentifier: String): Clause = {
      val termsHead = atomA.getTerms.asScala.union(atomB.getTerms.asScala).distinct
      val atomHead: Atom = new DefaultAtom(new Predicate(predicateIdentifier, termsHead.length))
      termsHead.zipWithIndex.foreach {
        case (term, index) => atomHead.setTerm(index, term)
      }
      Clause(atomHead, List(atomA, atomB))
    }
  }

}