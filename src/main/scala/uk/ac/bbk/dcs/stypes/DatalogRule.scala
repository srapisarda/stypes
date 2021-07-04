package uk.ac.bbk.dcs.stypes

/*
 * #%L
 * STypeS
 * %%
 * Copyright (C) 2017 - 2021 Birkbeck University of London
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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

  override def equals(obj: Any): Boolean = {
    if (obj == null || !obj.isInstanceOf[Clause])
      false
    else {
      val that = obj.asInstanceOf[Clause]
      that.head.equals(this.head) && that.body.equals(this.body)
    }
  }
}


trait BinaryOperator {
  def t1: Any

  def t2: Any
}

case class Equality(t1: Term, t2: Term) extends DefaultAtom(Equality.getAtom(t1, t2)) with BinaryOperator {
  def getAtom: Atom = this
}

object Equality {
  val predicateName: String = "EQ"

  def getAtom(t1: Term, t2: Term): Atom = new DefaultAtom(new Predicate(Equality.predicateName, 2), List(t1, t2).asJava)
}

case class DatalogPredicate(identifier: Any, arity: Int, isGoalPredicate: Boolean = false) extends Predicate(identifier, arity)

