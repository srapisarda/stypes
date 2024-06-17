package uk.ac.bbk.dcs.stypes.sql

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.api.factory.TermFactory
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.util.validation.feature.DatabaseType
import net.sf.jsqlparser.util.validation.Validation
import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.ReWriter
import uk.ac.bbk.dcs.stypes.utils.NdlUtils

import scala.collection.JavaConverters._
import scala.io.Source

class SqlUtilsTest extends FunSpec {
  describe("sql util tests") {

    it("should return a correct list of IDB predicate dependencies for qw01-rew_test.dlp") {
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/qw01-rew_test.dlp")
      val spanningTree = SqlUtils.getIdbTopologicalSorting(new Predicate("p1", 2), ndl)
      val expected = List("p2", "p3", "p1").map(new Predicate(_, 2))
      assert(spanningTree === expected)
    }

    it("should return a correct list of IDB predicate dependencies for q15-rew.dlp") {
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q15-rew.dlp")
      val spanningTree = SqlUtils.getIdbTopologicalSorting(new Predicate("p1", 2), ndl)
      val expected = List("p28", "p43", "p40", "p19", "p3", "p35", "p5", "p7", "p14", "p2", "p1").map(new Predicate(_, 2))
      assert(expected == spanningTree)
    }

    it("should return a correct list of IDB predicate dependencies for q22-rew_test.dlp") {
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q22-rew_test.dlp")
      val spanningTree = SqlUtils.getIdbTopologicalSorting(new Predicate("p1", 2), ndl)
      val expected = List("p12", "p3", "p1").map(new Predicate(_, 2))
      assert(expected == spanningTree)
    }

    it("should return the eDBs predicates the NDL contains") {
      val expected = Seq("a", "b", "s", "r")
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q15-rew.dlp")
      val eDbPredicates = NdlUtils.getEdbPredicates(ndl, None)
      val actual = eDbPredicates.map(_.getIdentifier)
      expected.foreach(exp => assert(actual.contains(exp)))
    }


    it("should return a statement for q01-rew_test.dlp") {
      commonAssertions("src/test/resources/rewriting/q01-rew_test.dlp")
    }

    it("should return a statement for q01-rew_test.dlp using with") {
      commonAssertions("src/test/resources/rewriting/q01-rew_test.dlp",
        new Predicate("p1", 2), useWith = true)
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

    it("should return a statement for q07-rew_test.dlp using with") {
      commonAssertions("src/test/resources/rewriting/q07-rew_test.dlp",
        new Predicate("p1", 1), useWith = true)
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

    it("should return a statement for q15-rew_test.dlp using with") {
      commonAssertions("src/test/resources/rewriting/q15-rew.dlp",
        new Predicate("p1", 2), useWith = true)
    }

    it("should return a statement for q15-rew-n_test.dlp using with") {
      commonAssertions("src/test/resources/rewriting/q15-rew-n.dlp",
        new Predicate("p1", 2), useWith = true, List("x", "y"), true, isForThesis = true)
    }

    it("should return a statement for q22-rew_test.dlp") {
      commonAssertions("src/test/resources/rewriting/q22-rew_test.dlp")
    }

    it("should return a statement for q22-rew_test-with.dlp using with") {
      commonAssertions("src/test/resources/rewriting/q22-rew_test.dlp",
        new Predicate("p1", 2), useWith = true)
    }

    it("should return a statement for q22-rew_test-with.dlp using with alias") {
      commonAssertions("src/test/resources/rewriting/q22-rew_test.dlp",
        new Predicate("p1", 2), useWith = true, List("x", "y"))
    }

    it("should return a statement for q45-rew_test-with.dlp using with alias") {
      commonAssertions("src/test/resources/rewriting/q45-rew_test.dlp",
        new Predicate("p1", 2), useWith = true, List("x", "y"))
    }

    it("should return a statement for example-rew-q22-p3-res.dlp using with alias") {
      commonAssertions("src/test/resources/rewriting/example-rew-q22-p3-res.dlp",
        new Predicate("p1", 2), useWith = true, List("x", "y"), true)
    }
  }

  describe("sql thesis tests") {

    it("should return a statement for thesis q01-rew_test.dlp") {
      commonAssertions("src/test/resources/rewriting/thesis/q01-rew_test.dlp",
        new Predicate("p1", 1), useWith = false, List("x"), true, isForThesis = true)
    }

    it("should return a statement for thesis q01-rew_test.dlp with alias") {
      commonAssertions("src/test/resources/rewriting/thesis/q01-rew_test.dlp",
        new Predicate("p1", 1), useWith = true, List("x"), false, isForThesis = true)
    }

    it("should return a statement for thesis q02-rew_test.dlp") {
      commonAssertions("src/test/resources/rewriting/thesis/q02-rew_test.dlp",
        new Predicate("p1", 2), useWith = false, List("x, y"), true, isForThesis = true)
    }

    //todo fix test
    ignore("should return a statement for thesis q02-rew_test.dlp with alias") {
      commonAssertions("src/test/resources/rewriting/thesis/q02-rew_test.dlp",
        new Predicate("p1", 2), useWith = true, List("x, y"), true, isForThesis = true)
    }

    it("should return a correct list of IDB predicate dependencies for q15-rew.dlp") {
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/thesis/q15-rew.dlp")
      val topologicalSorting = SqlUtils.getIdbTopologicalSorting(new Predicate("p1", 2), ndl)
      val expected = List("p28", "p43", "p40", "p19", "p3", "p35", "p5", "p7", "p14", "p2", "p1").map(new Predicate(_, 2))
      assert(expected == topologicalSorting)
    }

    it("should return a correct list of IDB predicate dependencies for q-complexity-test.dlp") {
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/thesis/q-complexity-test.dlp")
      val topologicalSorting = SqlUtils.getIdbTopologicalSorting(new Predicate("p1", 2), ndl)
      val expected = List("p10", "p9", "p8", "p7", "p6", "p5",  "p4", "p3", "p2", "p1").map(new Predicate(_, 2))
      assert(expected == topologicalSorting)
    }


  }
  private def commonAssertions(datalogFileRewriting: String,
                               goalPredicate: Predicate = new Predicate("p1", 2),
                               useWith: Boolean = false,
                               selectAlias: List[String] = Nil,
                               isLowerCaseArgs:Boolean = false,
                               isForThesis:Boolean=false): Unit = {

    val sUseWith = if (useWith) "-with" else ""
    val sAlias = if (selectAlias.nonEmpty) "-alias" else ""
    val expectedFileName = datalogFileRewriting.replace(".dlp", s"$sUseWith$sAlias.sql")
    val expectedFile = Source.fromFile(expectedFileName)
    val sqlExpected = expectedFile.getLines().map(_.trim).mkString(" ")
    expectedFile.close()
    val stmt = CCJSqlParserUtil.parse(sqlExpected)

    val ndl = ReWriter.getDatalogRewriting(datalogFileRewriting)
    val actual = SqlUtils.ndl2sql(ndl, goalPredicate, getEDBCatalog(isLowerCaseArgs, isForThesis), useWith, selectAlias)
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

  private def getEDBCatalog(isLowerCase:Boolean= false, isForThesis:Boolean=false): EDBCatalog = {
    val checkWithLowerCase = (identifier: String) => if (isLowerCase) identifier.toLowerCase else identifier

    val tf: TermFactory = DefaultTermFactory.instance
    val term1 = if (!isForThesis) "X" else "A1"
    val term2 = if (!isForThesis) "Y" else "A2"

    val x: Term = tf.createVariable(checkWithLowerCase(term1))
    val y: Term = tf.createVariable(checkWithLowerCase(term2))

    val r: Atom = new DefaultAtom(new Predicate("r", 2), List(x, y).asJava)
    val s: Atom = new DefaultAtom(new Predicate("s", 2), List(x, y).asJava)
    val t: Atom = new DefaultAtom(new Predicate("t", 2), List(x, y).asJava)
    val a: Atom = new DefaultAtom(new Predicate("a", 1), List(x).asJava)
    val b: Atom = new DefaultAtom(new Predicate("b", 1), List(x).asJava)

    EDBCatalog(Set(a, b, r, s, t))
  }

}
