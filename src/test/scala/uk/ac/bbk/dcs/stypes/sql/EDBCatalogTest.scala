package uk.ac.bbk.dcs.stypes.sql

import fr.lirmm.graphik.graal.api.core.Predicate
import fr.lirmm.graphik.graal.api.factory.TermFactory
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.scalatest.FunSpec

class EDBCatalogTest extends FunSpec {
  describe("EDBCatalog Tests") {

    val tf: TermFactory = DefaultTermFactory.instance
    val x = tf.createVariable("x")
    val y = tf.createVariable("y")
    val rp = new Predicate("r", 2)
    val sp = new Predicate("s", 2)
    val ap = new Predicate("a", 1)
    val bp = new Predicate("b", 1)
    val tp = new Predicate("t", 1)

    val a = new DefaultAtom(ap, x)
    val b = new DefaultAtom(bp, x)
    val r = new DefaultAtom(rp, x, y)
    val s = new DefaultAtom(sp, x, y)
    val t = new DefaultAtom(tp, x)

    it("should parse a the set of atoms as a EDB catalog") {
      val filename = "src/test/resources/EDBCatalog/edb-catalog.dpl"
      val actual = EDBCatalog.getEDBCatalogFromFile(filename)
      assert(actual === EDBCatalog(Set(s, r, a, b)))
    }

    it("should parse a the set of atoms as a EDB catalog not expected") {
      val filename = "src/test/resources/EDBCatalog/edb-catalog.dpl"
      val actual = EDBCatalog.getEDBCatalogFromFile(filename)
      assert(actual !== EDBCatalog(Set(s, r, a, b, t)))
    }

  }
}
