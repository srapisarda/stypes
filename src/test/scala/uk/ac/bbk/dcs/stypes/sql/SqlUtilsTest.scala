package uk.ac.bbk.dcs.stypes.sql

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.api.factory.TermFactory
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import net.sf.jsqlparser.util.validation.feature.DatabaseType
import net.sf.jsqlparser.util.validation.Validation
import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.ReWriter

import scala.collection.JavaConverters._

class SqlUtilsTest extends FunSpec {
  describe("sql util tests") {
    it("should return the eDBs predicates the NDL contains") {
      val expected = Seq("a", "b", "s", "r")
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q15-rew.dlp")
      val eDbPredicates = SqlUtils.getEdbPredicates(ndl, None)
      val actual = eDbPredicates.map(_.getIdentifier)
      expected.foreach(exp => assert(actual.contains(exp)))
    }


    it("should return a statement for q01-rew_test.dlp") {
      // p1(x0,x1) :- a(x0), r(x0, x1), b(x1).
      val sqlExpected = "SELECT a0.X AS X0, r1.Y AS X1 FROM a AS a0 " +
        "INNER JOIN r AS r1 ON a0.X = r1.X " +
        "INNER JOIN b AS b2 ON r1.Y = b2.X"

      commonAssertions("src/test/resources/rewriting/q01-rew_test.dlp", sqlExpected)
    }

    it("should return a statement for q02-rew_test.dlp") {
      // p1(x0,x1) :- a(x0), r(x0, x1), b(x1).
      // p1(x0,x2) :- r(x0, x1), s(x1, x2), b(x2).
      val sqlExpected = "SELECT a0.X AS X0, r1.Y AS X1 FROM a AS a0 " +
        "INNER JOIN r AS r1 ON a0.X = r1.X " +
        "INNER JOIN b AS b2 ON r1.Y = b2.X " +
        "UNION " +
        "(SELECT r0.X AS X0, s1.Y AS X1 FROM r AS r0 " +
        "INNER JOIN s AS s1 ON r0.Y = s1.X " +
        "INNER JOIN b AS b2 ON s1.Y = b2.X)"

      commonAssertions("src/test/resources/rewriting/q02-rew_test.dlp", sqlExpected)
    }


    it("should return a statement for q03-rew_test.dlp") {
      // p1(x0,x1) :- a(x0), r(x0, x1), b(x1).
      // p1(x0,x3) :- r(x0, x1), s(x1, x2), p2(x2, x3).
      // p2(x0,x1) :- a(x0), r(x0, x1), b(x1).
      val sqlExpected = "SELECT a0.X AS X0, r1.Y AS X1 FROM a AS a0 " +
        "INNER JOIN r AS r1 ON a0.X = r1.X " +
        "INNER JOIN b AS b2 ON r1.Y = b2.X " +
        "UNION " +
        "(SELECT r0.X AS X0, p22.X1 AS X1 FROM r AS r0 " +
        "INNER JOIN s AS s1 ON r0.Y = s1.X " +
        "INNER JOIN " +
        "(SELECT a0.X AS X0, r1.Y AS X1 FROM a AS a0 " +
        "INNER JOIN r AS r1 ON a0.X = r1.X " +
        "INNER JOIN b AS b2 ON r1.Y = b2.X) AS p22 ON s1.Y = p22.X0)"

      commonAssertions("src/test/resources/rewriting/q03-rew_test.dlp", sqlExpected)
    }

    it("should return a statement for q04-rew_test.dlp") {
      //  p1(x0,x1) :- a(x0), p2(x0, x1), b(x1).
      //  p2(x0,x3) :- r(x0, x1), s(x1, x2), p3(x2, x3).
      //  p3(x0,x1) :- a(x0), r(x0, x1), b(x1).
      val sqlExpected = "SELECT a0.X AS X0, p21.X1 AS X1 FROM a AS a0 " +
        "INNER JOIN " +
        "(SELECT r0.X AS X0, p32.X1 AS X1 FROM r AS r0 " +
        "INNER JOIN s AS s1 ON r0.Y = s1.X " +
        "INNER JOIN " +
        "(SELECT a0.X AS X0, r1.Y AS X1 FROM a AS a0 " +
        "INNER JOIN r AS r1 ON a0.X = r1.X " +
        "INNER JOIN b AS b2 ON r1.Y = b2.X) AS p32 " +
        "ON s1.Y = p32.X0) AS p21 ON a0.X = p21.X0 " +
        "INNER JOIN b AS b2 ON p21.X1 = b2.X"

      commonAssertions("src/test/resources/rewriting/q04-rew_test.dlp", sqlExpected)
    }

    it("should return a statement for q05-rew_test.dlp") {
      // p1(x0, x4) :- a(x0), r(x0, x1), b(x1), r(x1, x2), s(x2, x3), s(x3, x4).
      val sqlExpected = "SELECT a0.X AS X0, s5.Y AS X1 " +
        "FROM a AS a0 " +
        "INNER JOIN r AS r1 ON a0.X = r1.X " +
        "INNER JOIN b AS b2 ON r1.Y = b2.X " +
        "INNER JOIN r AS r3 ON r1.Y = r3.X " +
        "INNER JOIN s AS s4 ON r3.Y = s4.X " +
        "INNER JOIN s AS s5 ON s4.Y = s5.X"

      commonAssertions("src/test/resources/rewriting/q05-rew_test.dlp", sqlExpected)
    }

    it("should return a statement for q06-rew_test.dlp") {
      // p1(x0, x4) :- a(x0), s(x3, x4), r(x0, x1), r(x1, x2), s(x2, x3), b(x1).
      val sqlExpected = "SELECT a0.X AS X0, s5.Y " +
        "AS X1 FROM a AS a0 " +
        "INNER JOIN r AS r1 ON a0.X = r1.X " +
        "INNER JOIN r AS r2 ON r1.Y = r2.X " +
        "INNER JOIN b AS b3 ON r1.Y = b3.X " +
        "INNER JOIN s AS s4 ON r2.Y = s4.X " +
        "INNER JOIN s AS s5 ON s4.Y = s5.X"

      commonAssertions("src/test/resources/rewriting/q06-rew_test.dlp", sqlExpected)
    }


    it("should return a statement for q07-rew_test.dlp") {
      // p1(x0, x4) :- a(x0), s(x3, x4), r(x0, x1), r(x1, x2), s(x2, x3), b(x1).
      val sqlExpected = "SELECT p30.X0 AS X0 FROM (SELECT s0.X AS X0 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X INNER JOIN (SELECT a0.X AS X0 FROM a AS a0 UNION (SELECT s0.X AS X0 FROM s AS s0)) AS p92 ON r1.Y = p92.X0 UNION (SELECT s0.X AS X0 FROM s AS s0 INNER JOIN b AS b1 ON s0.Y = b1.X) UNION (SELECT a0.X AS X0 FROM a AS a0 INNER JOIN (SELECT a0.X AS X0 FROM a AS a0 UNION (SELECT s0.X AS X0 FROM s AS s0)) AS p91 ON a0.X = p91.X0)) AS p30 INNER JOIN (SELECT r1.Y AS X0 FROM (SELECT s0.Y AS X0 FROM s AS s0 UNION (SELECT b0.X AS X0 FROM b AS b0)) AS p60 INNER JOIN r AS r1 ON p60.X0 = r1.X INNER JOIN b AS b2 ON r1.Y = b2.X UNION (SELECT a0.X AS X0 FROM a AS a0 INNER JOIN b AS b1 ON a0.X = b1.X)) AS p21 ON p30.X0 = p21.X0 INNER JOIN b AS b2 ON p30.X0 = b2.X UNION (SELECT s1.Y AS X0 FROM (SELECT r2.Y AS X0 FROM (SELECT s0.Y AS X0 FROM s AS s0 UNION (SELECT b0.X AS X0 FROM b AS b0)) AS p60 INNER JOIN r AS r1 ON p60.X0 = r1.X INNER JOIN r AS r2 ON r1.Y = r2.X UNION (SELECT r1.Y AS X0 FROM a AS a0 INNER JOIN r AS r1 ON a0.X = r1.X)) AS p120 INNER JOIN s AS s1 ON p120.X0 = s1.X INNER JOIN (SELECT s0.X AS X0 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X INNER JOIN (SELECT a0.X AS X0 FROM a AS a0 UNION (SELECT s0.X AS X0 FROM s AS s0)) AS p92 ON r1.Y = p92.X0 UNION (SELECT s0.X AS X0 FROM s AS s0 INNER JOIN b AS b1 ON s0.Y = b1.X) UNION (SELECT a0.X AS X0 FROM a AS a0 INNER JOIN (SELECT a0.X AS X0 FROM a AS a0 UNION (SELECT s0.X AS X0 FROM s AS s0)) AS p91 ON a0.X = p91.X0)) AS p32 ON s1.Y = p32.X0)"

      commonAssertions("src/test/resources/rewriting/q07-rew_test.dlp", sqlExpected, new Predicate("p1", 1))
    }

    it("should return a statement for q07p2-rew_test.dlp") {
      // p1(X2) :- b(X2), p6(X1), r(X1,X2).
      // p6(X1) :- s(X0,X1).
      // p6(X1) :- b(X1).
      val sqlExpected =
      "SELECT r1.Y AS X0 FROM " +
        "(SELECT s0.Y AS X0 FROM s AS s0 " +
        "UNION (SELECT b0.X AS X0 FROM b AS b0)) AS p60 " +
        "INNER JOIN r AS r1 ON p60.X0 = r1.X " +
        "INNER JOIN b AS b2 ON r1.Y = b2.X"

      commonAssertions("src/test/resources/rewriting/q07p2-rew_test.dlp", sqlExpected, new Predicate("p1", 1))
    }


    it("should return a statement for q07p3-rew_test.dlp") {
      //p1(X4) :- p3(X4), b(X4).
      //p3(X4) :- r(X5,X6), s(X4,X5), b(X6).
      //p3(X4) :- s(X4,X7), b(X7).
      //p3(X4) :- a(X4), b(X4).

      val sqlExpected =
        "SELECT p30.X0 AS X0 FROM " +
          "(SELECT s0.X AS X0 FROM s AS s0 " +
          "INNER JOIN r AS r1 ON s0.Y = r1.X " +
          "INNER JOIN b AS b2 ON r1.Y = b2.X " +
          "UNION (SELECT s0.X AS X0 FROM s AS s0 INNER JOIN b AS b1 ON s0.Y = b1.X) " +
          "UNION (SELECT a0.X AS X0 FROM a AS a0 INNER JOIN b AS b1 ON a0.X = b1.X)) AS p30 " +
          "INNER JOIN b AS b1 ON p30.X0 = b1.X"

      commonAssertions("src/test/resources/rewriting/q07p3-rew_test.dlp", sqlExpected, new Predicate("p1", 1))
    }

    it("should return a statement for q15-rew_test.dlp") {
      // p1(x0, x4) :- a(x0), s(x3, x4), r(x0, x1), r(x1, x2), s(x2, x3), b(x1).
      val sqlExpected = "SELECT p350.X0 AS X0, p22.X1 AS X1 FROM (SELECT r0.X AS X0, p432.X1 AS X1 FROM r AS r0 INNER JOIN r AS r1 ON r0.Y = r1.X INNER JOIN (SELECT s2.Y AS X0, s0.X AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X INNER JOIN s AS s2 ON r1.Y = s2.X UNION (SELECT s0.Y AS X0, s0.X AS X1 FROM s AS s0 INNER JOIN b AS b1 ON s0.Y = b1.X) UNION (SELECT s1.Y AS X0, a0.X AS X1 FROM a AS a0 INNER JOIN s AS s1 ON a0.X = s1.X)) AS p432 ON r1.Y = p432.X1 INNER JOIN a AS a3 ON r1.Y = a3.X UNION (SELECT p190.X0 AS X0, p432.X1 AS X1 FROM (SELECT r0.X AS X0, r0.Y AS X1 FROM r AS r0 INNER JOIN b AS b1 ON r0.Y = b1.X UNION (SELECT r0.X AS X0, s2.Y AS X1 FROM r AS r0 INNER JOIN r AS r1 ON r0.Y = r1.X INNER JOIN s AS s2 ON r1.Y = s2.X)) AS p190 INNER JOIN r AS r1 ON p190.X1 = r1.X INNER JOIN (SELECT s2.Y AS X0, s0.X AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X INNER JOIN s AS s2 ON r1.Y = s2.X UNION (SELECT s0.Y AS X0, s0.X AS X1 FROM s AS s0 INNER JOIN b AS b1 ON s0.Y = b1.X) UNION (SELECT s1.Y AS X0, a0.X AS X1 FROM a AS a0 INNER JOIN s AS s1 ON a0.X = s1.X)) AS p432 ON r1.Y = p432.X1) UNION (SELECT p190.X0 AS X0, p401.X1 AS X1 FROM (SELECT r0.X AS X0, r0.Y AS X1 FROM r AS r0 INNER JOIN b AS b1 ON r0.Y = b1.X UNION (SELECT r0.X AS X0, s2.Y AS X1 FROM r AS r0 INNER JOIN r AS r1 ON r0.Y = r1.X INNER JOIN s AS s2 ON r1.Y = s2.X)) AS p190 INNER JOIN (SELECT s2.Y AS X0, b0.X AS X1 FROM b AS b0 INNER JOIN r AS r1 ON b0.X = r1.X INNER JOIN s AS s2 ON r1.Y = s2.X UNION (SELECT b0.X AS X0, b0.X AS X1 FROM b AS b0)) AS p401 ON p190.X1 = p401.X1 INNER JOIN b AS b2 ON p190.X1 = b2.X)) AS p350 INNER JOIN r AS r1 ON p350.X1 = r1.X INNER JOIN (SELECT p51.X0 AS X0, p142.X1 AS X1 FROM r AS r0 INNER JOIN (SELECT b0.X AS X0, b0.X AS X1 FROM b AS b0 UNION (SELECT s0.Y AS X0, r1.X AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.X = r1.Y)) AS p51 ON r0.X = p51.X0 INNER JOIN (SELECT r0.X AS X0, p72.X1 AS X1 FROM r AS r0 INNER JOIN s AS s1 ON r0.Y = s1.X INNER JOIN (SELECT s0.X AS X0, r1.Y AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X UNION (SELECT a0.X AS X0, a0.X AS X1 FROM a AS a0)) AS p72 ON s1.Y = p72.X0 UNION (SELECT b0.X AS X0, p71.X1 AS X1 FROM b AS b0 INNER JOIN (SELECT s0.X AS X0, r1.Y AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X UNION (SELECT a0.X AS X0, a0.X AS X1 FROM a AS a0)) AS p71 ON b0.X = p71.X0)) AS p142 ON r0.Y = p142.X0 UNION (SELECT r1.X AS X0, p140.X1 AS X1 FROM (SELECT r0.X AS X0, p72.X1 AS X1 FROM r AS r0 INNER JOIN s AS s1 ON r0.Y = s1.X INNER JOIN (SELECT s0.X AS X0, r1.Y AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X UNION (SELECT a0.X AS X0, a0.X AS X1 FROM a AS a0)) AS p72 ON s1.Y = p72.X0 UNION (SELECT b0.X AS X0, p71.X1 AS X1 FROM b AS b0 INNER JOIN (SELECT s0.X AS X0, r1.Y AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X UNION (SELECT a0.X AS X0, a0.X AS X1 FROM a AS a0)) AS p71 ON b0.X = p71.X0)) AS p140 INNER JOIN r AS r1 ON p140.X0 = r1.Y INNER JOIN a AS a2 ON p140.X0 = a2.X)) AS p22 ON r1.Y = p22.X0 UNION (SELECT p350.X0 AS X0, p350.X1 AS X1 FROM (SELECT r0.X AS X0, p432.X1 AS X1 FROM r AS r0 INNER JOIN r AS r1 ON r0.Y = r1.X INNER JOIN (SELECT s2.Y AS X0, s0.X AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X INNER JOIN s AS s2 ON r1.Y = s2.X UNION (SELECT s0.Y AS X0, s0.X AS X1 FROM s AS s0 INNER JOIN b AS b1 ON s0.Y = b1.X) UNION (SELECT s1.Y AS X0, a0.X AS X1 FROM a AS a0 INNER JOIN s AS s1 ON a0.X = s1.X)) AS p432 ON r1.Y = p432.X1 INNER JOIN a AS a3 ON r1.Y = a3.X UNION (SELECT p190.X0 AS X0, p432.X1 AS X1 FROM (SELECT r0.X AS X0, r0.Y AS X1 FROM r AS r0 INNER JOIN b AS b1 ON r0.Y = b1.X UNION (SELECT r0.X AS X0, s2.Y AS X1 FROM r AS r0 INNER JOIN r AS r1 ON r0.Y = r1.X INNER JOIN s AS s2 ON r1.Y = s2.X)) AS p190 INNER JOIN r AS r1 ON p190.X1 = r1.X INNER JOIN (SELECT s2.Y AS X0, s0.X AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X INNER JOIN s AS s2 ON r1.Y = s2.X UNION (SELECT s0.Y AS X0, s0.X AS X1 FROM s AS s0 INNER JOIN b AS b1 ON s0.Y = b1.X) UNION (SELECT s1.Y AS X0, a0.X AS X1 FROM a AS a0 INNER JOIN s AS s1 ON a0.X = s1.X)) AS p432 ON r1.Y = p432.X1) UNION (SELECT p190.X0 AS X0, p401.X1 AS X1 FROM (SELECT r0.X AS X0, r0.Y AS X1 FROM r AS r0 INNER JOIN b AS b1 ON r0.Y = b1.X UNION (SELECT r0.X AS X0, s2.Y AS X1 FROM r AS r0 INNER JOIN r AS r1 ON r0.Y = r1.X INNER JOIN s AS s2 ON r1.Y = s2.X)) AS p190 INNER JOIN (SELECT s2.Y AS X0, b0.X AS X1 FROM b AS b0 INNER JOIN r AS r1 ON b0.X = r1.X INNER JOIN s AS s2 ON r1.Y = s2.X UNION (SELECT b0.X AS X0, b0.X AS X1 FROM b AS b0)) AS p401 ON p190.X1 = p401.X1 INNER JOIN b AS b2 ON p190.X1 = b2.X)) AS p350 INNER JOIN b AS b1 ON p350.X1 = b1.X) UNION (SELECT p30.X0 AS X0, p21.X1 AS X1 FROM (SELECT r0.X AS X0, p282.X1 AS X1 FROM r AS r0 INNER JOIN r AS r1 ON r0.Y = r1.X INNER JOIN (SELECT s0.X AS X0, r1.Y AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X INNER JOIN a AS a2 ON r1.Y = a2.X UNION (SELECT a0.X AS X0, a0.X AS X1 FROM a AS a0)) AS p282 ON r1.Y = p282.X0 INNER JOIN a AS a3 ON r1.Y = a3.X UNION (SELECT p190.X0 AS X0, r2.Y AS X1 FROM (SELECT r0.X AS X0, r0.Y AS X1 FROM r AS r0 INNER JOIN b AS b1 ON r0.Y = b1.X UNION (SELECT r0.X AS X0, s2.Y AS X1 FROM r AS r0 INNER JOIN r AS r1 ON r0.Y = r1.X INNER JOIN s AS s2 ON r1.Y = s2.X)) AS p190 INNER JOIN b AS b1 ON p190.X1 = b1.X INNER JOIN r AS r2 ON p190.X1 = r2.X INNER JOIN a AS a3 ON r2.Y = a3.X) UNION (SELECT p190.X0 AS X0, p282.X1 AS X1 FROM (SELECT r0.X AS X0, r0.Y AS X1 FROM r AS r0 INNER JOIN b AS b1 ON r0.Y = b1.X UNION (SELECT r0.X AS X0, s2.Y AS X1 FROM r AS r0 INNER JOIN r AS r1 ON r0.Y = r1.X INNER JOIN s AS s2 ON r1.Y = s2.X)) AS p190 INNER JOIN r AS r1 ON p190.X1 = r1.X INNER JOIN (SELECT s0.X AS X0, r1.Y AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X INNER JOIN a AS a2 ON r1.Y = a2.X UNION (SELECT a0.X AS X0, a0.X AS X1 FROM a AS a0)) AS p282 ON r1.Y = p282.X0)) AS p30 INNER JOIN (SELECT p51.X0 AS X0, p142.X1 AS X1 FROM r AS r0 INNER JOIN (SELECT b0.X AS X0, b0.X AS X1 FROM b AS b0 UNION (SELECT s0.Y AS X0, r1.X AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.X = r1.Y)) AS p51 ON r0.X = p51.X0 INNER JOIN (SELECT r0.X AS X0, p72.X1 AS X1 FROM r AS r0 INNER JOIN s AS s1 ON r0.Y = s1.X INNER JOIN (SELECT s0.X AS X0, r1.Y AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X UNION (SELECT a0.X AS X0, a0.X AS X1 FROM a AS a0)) AS p72 ON s1.Y = p72.X0 UNION (SELECT b0.X AS X0, p71.X1 AS X1 FROM b AS b0 INNER JOIN (SELECT s0.X AS X0, r1.Y AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X UNION (SELECT a0.X AS X0, a0.X AS X1 FROM a AS a0)) AS p71 ON b0.X = p71.X0)) AS p142 ON r0.Y = p142.X0 UNION (SELECT r1.X AS X0, p140.X1 AS X1 FROM (SELECT r0.X AS X0, p72.X1 AS X1 FROM r AS r0 INNER JOIN s AS s1 ON r0.Y = s1.X INNER JOIN (SELECT s0.X AS X0, r1.Y AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X UNION (SELECT a0.X AS X0, a0.X AS X1 FROM a AS a0)) AS p72 ON s1.Y = p72.X0 UNION (SELECT b0.X AS X0, p71.X1 AS X1 FROM b AS b0 INNER JOIN (SELECT s0.X AS X0, r1.Y AS X1 FROM s AS s0 INNER JOIN r AS r1 ON s0.Y = r1.X UNION (SELECT a0.X AS X0, a0.X AS X1 FROM a AS a0)) AS p71 ON b0.X = p71.X0)) AS p140 INNER JOIN r AS r1 ON p140.X0 = r1.Y INNER JOIN a AS a2 ON p140.X0 = a2.X)) AS p21 ON p30.X1 = p21.X0 INNER JOIN a AS a2 ON p30.X1 = a2.X)"

      commonAssertions("src/test/resources/rewriting/q15-rew.dlp", sqlExpected)
    }

  }

  private def commonAssertions(datalogFileRewriting: String, sqlExpected: String,
                               goalPredicate: Predicate = new Predicate("p1", 2)): Unit = {
    //    val stmt = CCJSqlParserUtil.parseStatements(sqlExpected)
    val ndl = ReWriter.getDatalogRewriting(datalogFileRewriting)
    val actual = SqlUtils.ndl2sql(ndl, goalPredicate, getEDBCatalog)
    val sqlActual = actual.toString
    println(ndl)
    println("----")
    println(sqlActual)
    assert(isValidSql(sqlActual))
    assert(sqlActual === sqlExpected)
  }

  private def isValidSql(sql: String) = {
    val validation = new Validation(List(DatabaseType.POSTGRESQL).asJava, sql)
    val errors = validation.validate()
    println(errors)
    errors.isEmpty
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
