package uk.ac.bbk.dcs.stypes

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
