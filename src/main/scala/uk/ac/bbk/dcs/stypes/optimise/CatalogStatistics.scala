package uk.ac.bbk.dcs.stypes.optimise

import fr.lirmm.graphik.graal.api.core.Atom

case class CatalogStatistics(atomStatisticMap: Map[Atom, EdbStatistic]) {
  def getRowCount(atom: Atom) : Int = if (atomStatisticMap.contains(atom) ) atomStatisticMap(atom).rowCount else 100;
}
