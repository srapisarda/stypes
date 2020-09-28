package uk.ac.bbk.dcs.stypes.optimise

import uk.ac.bbk.dcs.stypes.Clause

trait OptimisationRule {
  def optimise(datalog: List[Clause], statistics: EDBsStatistics): List[Clause]
}
