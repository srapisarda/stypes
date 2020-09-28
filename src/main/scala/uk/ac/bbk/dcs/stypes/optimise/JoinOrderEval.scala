package uk.ac.bbk.dcs.stypes.optimise

import uk.ac.bbk.dcs.stypes.Clause

class JoinOrderEval extends OptimisationRule {

  override def optimise(datalog: List[Clause], statistics: EDBsStatistics): List[Clause] = {
    datalog
  }

}
