package uk.ac.bbk.dcs.stypes

import java.util

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.api.factory.TermFactory
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.scalatest._


/**
  * Created by rapissal on 24/04/2017.
  */
class BagTest extends FunSpec {

  val tf: TermFactory = DefaultTermFactory.instance
  val rterms: util.List[Term] = new util.ArrayList[Term]
  rterms.add(tf.createVariable("X"))
  rterms.add(tf.createVariable("Y"))
  val rPredicate: Predicate = new Predicate("r", 2)
  val r: Atom = new DefaultAtom(rPredicate, rterms)

  val sterms: util.List[Term] = new util.ArrayList[Term]
  sterms.add(tf.createVariable("Y"))
  sterms.add(tf.createVariable("Z"))
  val sPredicate: Predicate = new Predicate("s", 2)
  val s: Atom = new DefaultAtom(sPredicate, sterms)

  val b: Bag = Bag(Set(r, s))

  describe("Bag basic test") {

    it("should create a new instance of bag") {
      print(b)
      assert(b.variables.size == 3)
    }

    it ("to string returned as expected") {
      assert(b.toString === "atoms: Set(r[2](X,Y), s[2](Y,Z))), variables: Set(X, Y, Z)")
    }
  }

}
