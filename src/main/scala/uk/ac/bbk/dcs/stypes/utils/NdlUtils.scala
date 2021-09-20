package uk.ac.bbk.dcs.stypes.utils

import fr.lirmm.graphik.graal.api.core.Predicate
import uk.ac.bbk.dcs.stypes.Clause

object NdlUtils {

  /**
    * The EDBs are atoms present in the body but not in the head of a clause.
    * The IDBs are atoms defined as the head of a clause.
    *
    * @return set of Predicate
    **/
  def getIdbPredicates(ndl: List[Clause]): Set[Predicate] = {
    ndl.map(p => p.head.getPredicate).toSet
  }

  def getEdbPredicates(ndl: List[Clause], optIDbPredicates: Option[Set[Predicate]] = None): Set[Predicate] = {
    val iDbPredicates = optIDbPredicates.getOrElse(getIdbPredicates(ndl))
    ndl.flatten(_.body.map(_.getPredicate).distinct)
      .filter(!iDbPredicates.contains(_)).toSet
  }

  def getGoalPredicate(ndl: List[Clause]): Predicate = {
    val iDBs = getIdbPredicates(ndl)
    val predicatesClauseBodyContains = ndl.flatten(clause => clause.body.map(atom => atom.getPredicate).toSet)
    iDBs.find(predicate => !predicatesClauseBodyContains.contains(predicate))
      .getOrElse(throw new RuntimeException("The NDL does not contains any goal predicate"))
  }

  def getDepthByIdbPredicate(ndl: List[Clause], goalPredicateOption: Option[Predicate] = None): Int = {
    val goalPredicate = goalPredicateOption.getOrElse(getGoalPredicate(ndl))
    val eDBs = getEdbPredicates(ndl)
    val ndlGroupedByPredicate: Map[Predicate, List[Predicate]] = ndl
      .groupBy(_.head.getPredicate)
      .map {
        case (predicate, clauses) =>
          predicate -> clauses.flatMap(_.body.map(_.getPredicate)).filterNot(eDBs).distinct
      }

    def bfs(toVisit: List[Predicate], visited: Set[Predicate], depth: Int): Int = toVisit match {
      case List() => depth
      case predicate :: tail =>
        if (visited.contains(predicate))
          bfs(tail, visited, depth)
        else
          bfs(tail ++ ndlGroupedByPredicate(predicate), visited + predicate, depth + 1)
    }

    bfs(List(goalPredicate), Set(), 0)
  }
}
