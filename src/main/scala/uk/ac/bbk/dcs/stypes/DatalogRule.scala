package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.Atom

/**
  * Created by rapissal on 17/09/2017.
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
    override def toString: String = head + " :- " + body
  }