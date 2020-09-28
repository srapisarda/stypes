package uk.ac.bbk.dcs.stypes.optimise

import uk.ac.bbk.dcs.stypes.Clause

object Optimiser {

  @scala.annotation.tailrec
  def optimise(datalog: List[Clause], optimisationRules: List[OptimisationRule], statistics : EDBsStatistics): List[Clause] = optimisationRules match {
    case List() => datalog;
    case rule :: tail => optimise(datalog = rule.optimise(datalog, statistics), optimisationRules = tail, statistics)
  }

}
