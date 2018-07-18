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


import java.util

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.api.factory.TermFactory
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.scalatest._


/**
  * Created by Salvatore Rapisarda on 24/04/2017.
  */
class BagTest extends FunSpec {

  val tf: TermFactory = DefaultTermFactory.instance
  val rterms: util.List[Term] = new util.ArrayList[Term]

  val x = tf.createVariable("X")
  val y = tf.createVariable("Y")
  val z =tf.createVariable("Z")


  rterms.add(x)
  rterms.add(y)
  val rPredicate: Predicate = new Predicate("r", 2)
  val r: Atom = new DefaultAtom(rPredicate, rterms)

  val sterms: util.List[Term] = new util.ArrayList[Term]
  sterms.add(y)
  sterms.add(z)
  val sPredicate: Predicate = new Predicate("s", 2)
  val s: Atom = new DefaultAtom(sPredicate, sterms)

  val b: Bag = Bag(Set(r, s), Set(x, y, z ) )

  describe("The Bag basic test") {

    it("should create a new instance of bag") {
//      print(b)
      assert(b.variables.size == 3)
    }

    it ("method toString should returned as expected") {
      assert(b.toString === "(atoms: Set(r[2](X,Y), s[2](Y,Z)), variables: Set(X, Y, Z))")
    }
  }

}
