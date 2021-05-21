package uk.ac.bbk.dcs.stypes.sql

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate}

case class EDBCatalog(tables: Set[Atom]) {
  private lazy val predicateMapToAtom: Map[Predicate, Atom] = tables.map(atom => (atom.getPredicate, atom)).toMap

  def getAtomFromPredicate(p: Predicate): Option[Atom] =
    if (predicateMapToAtom.contains(p))
      Some(predicateMapToAtom(p))
    else
      None
}
