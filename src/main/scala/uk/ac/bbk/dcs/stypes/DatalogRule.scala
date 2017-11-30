package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.core.DefaultAtom

import scala.collection.JavaConverters._

/**
  *
  * Created by :
  * Salvatore Rapisarda
  * Stanislav Kikot
  *
  */

sealed trait DatalogRule {
  def head: Atom

  def name: String = head.getPredicate.toString

  def arity: Int = head.getPredicate.getArity

}

case class Fact(head: Atom) extends DatalogRule {
  override def toString: String = head.toString
}

case class Clause(head: Atom, body: List[Atom]) extends DatalogRule {
  override def toString: String = s"$head :- ${body.map(a => a).mkString(", ")}"
}

trait BinaryOperator  {
  def t1: Any
  def t2: Any
}

case class Equality(t1: Term, t2: Term) extends DefaultAtom(Equality.getAtom(t1, t2)) with BinaryOperator {
  def getAtom: Atom = this
}

object Equality {
  val predicateName: String = "EQ"
  def getAtom(t1: Term, t2: Term): Atom =  new DefaultAtom(new Predicate(Equality.predicateName, 2), List(t1, t2).asJava)
}

case class DatalogPredicate(identifier: Any, arity: Int, isGoalPredicate:Boolean=false ) extends Predicate(identifier, arity)



