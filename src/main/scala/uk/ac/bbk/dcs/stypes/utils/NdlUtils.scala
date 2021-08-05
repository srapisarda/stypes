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

  def getGoalPredicate(ndl: List[Clause]): Option[Predicate] = {
    val iDBs = getIdbPredicates(ndl)
    val predicatesClauseBodyContains = ndl.flatten(clause => clause.body.map(atom => atom.getPredicate).toSet)
    iDBs.find(predicate => !predicatesClauseBodyContains.contains(predicate))
  }

  def ndlDepth(ndl: List[Clause], goalPredicateOption: Option[Predicate] = None): Int = {

    0
  }
}
