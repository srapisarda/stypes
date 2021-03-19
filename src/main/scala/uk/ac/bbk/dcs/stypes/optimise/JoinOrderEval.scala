package uk.ac.bbk.dcs.stypes.optimise

import uk.ac.bbk.dcs.stypes.Clause

object JoinOrderEval extends OptimisationRule {

  override def optimise(datalog: List[Clause], statistics: CatalogStatistics): List[Clause] = {

    def optimiseH(datalog: List[Clause], statistics: CatalogStatistics, acc: List[Clause] = Nil): List[Clause] = datalog match {
      case Nil => acc
      case clause :: tail => optimiseH(tail, statistics, optimiseClauseJoinOrderEval(clause, statistics) :: acc)
    }

    optimiseH(datalog, statistics, List())
  }

  def optimiseClauseJoinOrderEval(clause: Clause, statistics: CatalogStatistics): Clause = {
     val atoms = clause.body
      .map(atom => (atom, statistics.atomStatisticMap(atom).rowCount))
      .sortBy(_._2)
      .map(_._1)

    Clause(clause.head , atoms)
  }
}
