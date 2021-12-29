package uk.ac.bbk.dcs.stypes.sql

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate}
import fr.lirmm.graphik.graal.io.dlp.DlgpParser

import java.io.File
import scala.collection.JavaConverters._

case class EDBCatalog(tables: Set[Atom]) {
  private lazy val predicateMapToAtom: Map[Predicate, Atom] = tables.map(atom => (atom.getPredicate, atom)).toMap

  def getAtomFromPredicate(p: Predicate): Option[Atom] =
    if (predicateMapToAtom.contains(p))
      Some(predicateMapToAtom(p))
    else
      None

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: EDBCatalog =>
        if (tables.size == that.tables.size) {
          that.tables
            .map(a1 => this.tables.exists(a2 => a1.getPredicate.equals(a2.getPredicate)))
            .reduce(_ && _)
        }
        else false
      case _ =>
        false
    }
  }
}

object EDBCatalog {
  def getEDBCatalogFromFile(filename: String): EDBCatalog = {
    val dlgpParser = new DlgpParser(new File(filename))
    val clauses: Set[Atom] = dlgpParser.asScala.map { case a: Atom => a }.toSet
    EDBCatalog(clauses)
  }

}