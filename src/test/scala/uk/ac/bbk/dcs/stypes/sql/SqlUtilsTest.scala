package uk.ac.bbk.dcs.stypes.sql

import fr.lirmm.graphik.graal.api.core.Predicate
import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.ReWriter

class SqlUtilsTest extends FunSpec {
  describe("sql util tests") {
    it("should return the eDBs predicates the NDL contains") {
      val expected = Seq("a", "b", "s", "r")
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q15-rew.dlp")
      val eDbPredicates = SqlUtils.getEdbPredicates(ndl)
      val actual = eDbPredicates.map(_.getIdentifier)
      expected.foreach(exp => assert(actual.contains(exp)))
    }

    it( "should return a statement for q01-rew_test.dlp") {
      //p1(x0,x3) :- a(x0,x1), b(x1, x2), r(x2, x3).
      val sqlExpected = "SELECT a0.x0, r2.x3 FROM a AS a0 " +
        "INNER JOIN b AS b1 on a0.x0 = b1.x0 " +
        "INNER JOIN r AS r2 on b1.x2 = r1.x2"
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q01-rew_test.dlp")
      val goalPredicate: Predicate = new Predicate("p1", 2)
      val actual = SqlUtils.ndl2sql(ndl, goalPredicate)
      val sqlActual = actual.toString
      assert(sqlActual===sqlExpected)
    }
  }
}
