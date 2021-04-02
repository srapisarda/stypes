package uk.ac.bbk.dcs.stypes.sql

import java.util

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.api.factory.TermFactory
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.ReWriter

import scala.collection.JavaConverters._

class SqlUtilsTest extends FunSpec {
  describe("sql util tests") {
    it("should return the eDBs predicates the NDL contains") {
      val expected = Seq("a", "b", "s", "r")
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q15-rew.dlp")
      val eDbPredicates = SqlUtils.getEdbPredicates(ndl)
      val actual = eDbPredicates.map(_.getIdentifier)
      expected.foreach(exp => assert(actual.contains(exp)))
    }


    it("should return a statement for q01-rew_test.dlp") {
      // p1(x0,x1) :- a(x0), r(x0, x1), b(x1).
      val sqlExpected = "SELECT a0.X, r1.Y FROM a AS a0 " +
        "INNER JOIN r AS r1 on a0.X = r1.X " +
        "INNER JOIN b AS b2 on r1.Y = b2.X"
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q01-rew_test.dlp")
      val goalPredicate: Predicate = new Predicate("p1", 2)
      val actual = SqlUtils.ndl2sql(ndl, goalPredicate, getEDBCatalog)
      val sqlActual = actual.toString
      println(ndl)
      println("----")
      println(sqlActual)

      assert(sqlActual === sqlExpected)
    }
  }

  private def getEDBCatalog: EDBCatalog = {
    val tf: TermFactory = DefaultTermFactory.instance

    val x: Term = tf.createVariable("X")
    val y: Term = tf.createVariable("Y")

    val r: Atom = new DefaultAtom(new Predicate("r", 2), List(x, y).asJava)
    val s: Atom = new DefaultAtom(new Predicate("s", 2), List(x, y).asJava)
    val a: Atom = new DefaultAtom(new Predicate("a", 1), List(x).asJava)
    val b: Atom = new DefaultAtom(new Predicate("b", 1), List(x).asJava)

    EDBCatalog(Set(a,b,r,s))
  }
}
