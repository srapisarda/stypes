package uk.ac.bbk.dcs.stypes.sql

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.api.factory.TermFactory
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.ReWriter

import scala.collection.JavaConverters._

class SqlUtilsTest extends FunSpec {
  describe("sql util tests") {
    it("should return the eDBs predicates the NDL contains") {
      val expected = Seq("a", "b", "s", "r")
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q15-rew.dlp")
      val eDbPredicates = SqlUtils.getEdbPredicates(ndl,None)
      val actual = eDbPredicates.map(_.getIdentifier)
      expected.foreach(exp => assert(actual.contains(exp)))
    }


    it("should return a statement for q01-rew_test.dlp") {
      // p1(x0,x1) :- a(x0), r(x0, x1), b(x1).
      val sqlExpected = "SELECT a0.X, r1.Y FROM a AS a0 " +
        "INNER JOIN r AS r1 ON a0.X = r1.X " +
        "INNER JOIN b AS b2 ON r1.Y = b2.X"
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q01-rew_test.dlp")
      val goalPredicate: Predicate = new Predicate("p1", 2)
      val actual = SqlUtils.ndl2sql(ndl, goalPredicate, getEDBCatalog)
      val sqlActual = actual.toString
      println(ndl)
      println("----")
      println(sqlActual)

      assert(sqlActual === sqlExpected)
    }

    it("should return a statement for q02-rew_test.dlp") {
      // p1(x0,x1) :- a(x0), r(x0, x1), b(x1).
      // p1(x0,x2) :- r(x0, x1), s(x1, x2), b(x2).
      val sqlExpected = "(SELECT a0.X, r1.Y FROM a AS a0 " +
        "INNER JOIN r AS r1 ON a0.X = r1.X " +
        "INNER JOIN b AS b2 ON r1.Y = b2.X) " +
        "UNION " +
        "(SELECT r0.X, s1.Y FROM r AS r0 " +
        "INNER JOIN s AS s1 ON r0.Y = s1.X " +
        "INNER JOIN b AS b2 ON s1.Y = b2.X)"
      val stmt = CCJSqlParserUtil.parseStatements(sqlExpected)

      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q02-rew_test.dlp")
      val goalPredicate: Predicate = new Predicate("p1", 2)
      val actual = SqlUtils.ndl2sql(ndl, goalPredicate, getEDBCatalog)
      val sqlActual = actual.toString
      println(ndl)
      println("----")
      println(sqlActual)

      assert(sqlActual === sqlExpected)
    }


    it("should return a statement for q03-rew_test.dlp") {
      // p1(x0,x1) :- a(x0), r(x0, x1), b(x1).
      // p1(x0,x3) :- r(x0, x1), s(x1, x2), p2(x2, x3).
      // p2(x0,x1) :- a(x0), r(x0, x1), b(x1).
      val sqlExpected = "(SELECT a0.X, r1.Y FROM a AS a0 " +
        "INNER JOIN r AS r1 ON a0.X = r1.X " +
        "INNER JOIN b AS b2 ON r1.Y = b2.X) " +
        "UNION " +
        "(SELECT r0.X, s1.Y FROM r AS r0 " +
        "INNER JOIN s AS s1 ON r0.Y = s1.X " +
        "INNER JOIN " +
        "(SELECT a0.X, b2.X FROM a AS a0 " +
        "INNER JOIN r AS r1 ON a0.X = r1.X " +
        "INNER JOIN b AS b2 ON r1.Y = b2.X) AS p22 ON  s1.Y = p22.x0)"
      val stmt = CCJSqlParserUtil.parseStatements(sqlExpected)

      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q03-rew_test.dlp")
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

    EDBCatalog(Set(a, b, r, s))
  }
}
