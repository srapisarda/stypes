package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.core.DefaultAtom
import scala.collection.JavaConverters._

/**
  *
  * Created by :
  *   Salvatore Rapisarda
  *   Stanislav Kikot
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
    override def toString: String = head + " :- " + body.map( a => a ).mkString(", ")
  }

  trait BinaryOperator  {
    def t1:Any
    def t2: Any
  }

  case class Equality ( t1: Term, t2: Term ) extends BinaryOperator{
    def getAtom :Atom =  new  DefaultAtom( new Predicate(Equality.predicateName,2 ), List(t1, t2).asJava  )
    override def toString: String = s"EQ($t1,$t2)" // not sure if should be t1=t2
  }
  object Equality {
    val predicateName:String ="EQ"
  }

