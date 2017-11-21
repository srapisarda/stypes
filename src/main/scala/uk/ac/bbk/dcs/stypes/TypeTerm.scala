package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core._
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import fr.lirmm.graphik.util.URI

/**
  * Created by
  *
  * Salvatore Rapisarda
  * Stanislav Kikot
  *
  * on 21/11/2017.
  */

object TypeTermFactory {

  def createOntologyVariable(identifier: Any): OntologyTerm = OntologyTerm(DefaultTermFactory.instance().createVariable(identifier))

  def createOntologyLiteral(value: Any): OntologyTerm  = OntologyTerm(DefaultTermFactory.instance().createLiteral(value))

  def createOntologyLiteral(datatype: URI, value: Any): OntologyTerm  = OntologyTerm(DefaultTermFactory.instance().createLiteral(datatype, value))

  def createOntologyConstant(identifier: Any) : OntologyTerm = OntologyTerm(DefaultTermFactory.instance().createConstant(identifier))

  def createQueryVariable(identifier: Any) : QueryTerm = QueryTerm(DefaultTermFactory.instance().createVariable(identifier))

  def createQueryLiteral(value: Any): QueryTerm = QueryTerm(DefaultTermFactory.instance().createLiteral(value))

  def createQueryLiteral(datatype: URI, value: Any): QueryTerm = QueryTerm(DefaultTermFactory.instance().createLiteral(datatype, value))

  def createQueryConstant(identifier: Any): QueryTerm = QueryTerm(DefaultTermFactory.instance().createConstant(identifier))

}

class TypeTerm(term: Term) extends AbstractTerm {

  override def getLabel: String = term.getLabel

  override def isConstant: Boolean = term.isConstant

  override def getType: Term.Type = term.getType

  override def getIdentifier: AnyRef = term.getIdentifier

  override def compareTo(o: Term): Int = term.compareTo(o)
}

case class OntologyTerm(term: Term) extends TypeTerm(term)

case class QueryTerm(term: Term) extends TypeTerm(term)