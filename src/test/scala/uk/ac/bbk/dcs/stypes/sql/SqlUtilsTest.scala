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
import scala.io.Source

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
      commonAssertions("src/test/resources/rewriting/q01-rew_test.dlp")
    }

    it("should return a statement for q02-rew_test.dlp") {
     commonAssertions("src/test/resources/rewriting/q02-rew_test.dlp")
    }


    it("should return a statement for q03-rew_test.dlp") {
      commonAssertions("src/test/resources/rewriting/q03-rew_test.dlp")
    }

    it("should return a statement for q04-rew_test.dlp") {
      commonAssertions("src/test/resources/rewriting/q04-rew_test.dlp")
    }

    it("should return a statement for q05-rew_test.dlp") {
      commonAssertions("src/test/resources/rewriting/q05-rew_test.dlp")
    }

    it("should return a statement for q06-rew_test.dlp") {
        commonAssertions("src/test/resources/rewriting/q06-rew_test.dlp")
    }

    it("should return a statement for q07-rew_test.dlp") {
      commonAssertions("src/test/resources/rewriting/q07-rew_test.dlp",
        new Predicate("p1", 1))
    }

    it("should return a statement for q07p2-rew_test.dlp") {
      commonAssertions("src/test/resources/rewriting/q07p2-rew_test.dlp",
        new Predicate("p1", 1))
    }


    it("should return a statement for q07p3-rew_test.dlp") {
      commonAssertions("src/test/resources/rewriting/q07p3-rew_test.dlp",
        new Predicate("p1", 1))
    }

    it("should return a statement for q15-rew_test.dlp") {
      commonAssertions("src/test/resources/rewriting/q15-rew.dlp")
    }

    it("should return a statement for q22-rew_test.dlp") {
      commonAssertions("src/test/resources/rewriting/q22-rew_test.dlp")
    }
  }

  private def commonAssertions(datalogFileRewriting: String,
                               goalPredicate: Predicate = new Predicate("p1", 2)): Unit = {

    val ndl = ReWriter.getDatalogRewriting(datalogFileRewriting)
    val actual = SqlUtils.ndl2sql(ndl, goalPredicate, getEDBCatalog)
    val sqlActual = actual.toString
    println(ndl)
    println("----")
    println(sqlActual)
    assert(isValidSql(sqlActual))
    val expectedFileName = datalogFileRewriting.replace(".dlp", ".sql")
    val expectedFile = Source.fromFile(expectedFileName)
    val sqlExpected = expectedFile.getLines().map(_.trim).mkString(" ")
    expectedFile.close()
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
