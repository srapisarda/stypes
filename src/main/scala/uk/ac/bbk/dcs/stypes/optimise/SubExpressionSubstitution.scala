package uk.ac.bbk.dcs.stypes.optimise

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.core.DefaultAtom
import uk.ac.bbk.dcs.stypes.Clause

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

object SubExpressionSubstitution extends OptimisationRule {
  val joinSharedPredicateIdentifier = "comp_"

  override def optimise(datalog: List[Clause], statistics: CatalogStatistics): List[Clause] = {
    val tuples = getJoinTuplesMapToClauses(datalog)
    val joinTuplesMapToClauses: Map[JoinTuple, List[Clause]] =
      tuples.filter { case (_, clauses) => clauses.length > 1 }.toMap

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
      val atoms = clauseToUpdate.body.filter(atom => {
        atom.getPredicate == joinTuples.atomA.getPredicate || atom.getPredicate == joinTuples.atomB.getPredicate
      })
      val atomCombinations = for {x <- atoms; y <- atoms; if x != y} yield (x, y)
      val atomsToSubstitute = atomCombinations.filter{ case (atomA, atomB) => {
        atomA.getPredicate == joinTuples.atomA.getPredicate &&
        atomB.getPredicate == joinTuples.atomB.getPredicate &&
        atomA.getTerm(joinTuples.positionTermA) == atomB.getTerm(joinTuples.positionTermB)
      }}.flatMap(t  => List(t._1, t._2))

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
                                        map: mutable.Map[JoinTuple, List[Clause]] =
                                        mutable.Map()): mutable.Map[JoinTuple, List[Clause]] = datalog match {
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

      joinTuples.map(jt => {
        if (map.contains(jt)) {
          map.put(jt, clause :: map(jt))
        } else {
          map.put(jt, clause :: Nil)
        }
      })

      getJoinTuplesMapToClauses(tail, map)
  }


  case class JoinTuple(atomA: Atom, atomB: Atom, positionTermA: Int, positionTermB: Int, term: Term) {

    override def hashCode(): Int = {
      s"${this.atomA.getPredicate.getIdentifier}_${this.atomB.getPredicate.getIdentifier}_${this.positionTermA}_${this.positionTermB}".hashCode
    }

    override def equals(obj: Any): Boolean = {
      if (obj == null) {
        false
      }
      else if (!obj.isInstanceOf[JoinTuple]) {
        false
      }
      else {
        val that = obj.asInstanceOf[JoinTuple]
        this.atomA.getPredicate == that.atomA.getPredicate &&
          this.atomB.getPredicate == that.atomB.getPredicate
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