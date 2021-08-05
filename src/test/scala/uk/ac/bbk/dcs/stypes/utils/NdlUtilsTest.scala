package uk.ac.bbk.dcs.stypes.utils

import fr.lirmm.graphik.graal.api.core.Predicate
import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.ReWriter

import scala.language.postfixOps

class NdlUtilsTest extends FunSpec {
  val folderPathTest = "src/test/resources/rewriting/"

  describe("Ndl Utils: get goal predicate") {

    it("should find the GOL predicate in q01-rew_test.dlp") {
      val expected = Some(new Predicate("p1", 2))
      assertGoalPredicate(expected, "q01-rew_test.dlp")
    }

    it("should find the GOL predicate in q02-rew_test.dlp") {
      val expected = Some(new Predicate("p1", 2))
      assertGoalPredicate(expected, "q02-rew_test.dlp")
    }

    it("should find the GOL predicate in q15-rew.dlp") {
      val expected = Some(new Predicate("p1", 2))
      assertGoalPredicate(expected, "q15-rew.dlp")
    }
  }

  describe("Ndl Utils: get IDBs") {

    it("should find all the IDBs in q01-rew_test.dlp") {
      val expected = Set(new Predicate("p1", 2))
      assertIdbPredicates(expected, "q01-rew_test.dlp")
    }

    it("should find all the IDBs in  in q02-rew_test.dlp") {
      val expected = Set(new Predicate("p1", 2))
      assertIdbPredicates(expected, "q02-rew_test.dlp")
    }

    it("should find all the IDBs in  in q15-rew.dlp") {
      val expected = List("p19", "p40", "p3", "p43", "p1", "p2", "p14", "p7", "p28", "p35", "p5")
        .map(new Predicate(_, 2))
        .toSet
      assertIdbPredicates(expected, "q15-rew.dlp")
    }

  }

  describe("Ndl Utils: get EDBs") {

    it("should find all the EDBs in q01-rew_test.dlp") {
      val expected = List(("a", 1), ("b", 1), ("r", 2))
        .map { case (identifier, arity) => new Predicate(identifier, arity) }
        .toSet
      assertEdbPredicates(expected, "q01-rew_test.dlp")
    }

    it("should find all the EDBs in q15-rew.dlp") {
      val expected = List(("a", 1), ("b", 1), ("r", 2), ("s", 2))
        .map { case (identifier, arity) => new Predicate(identifier, arity) }
        .toSet
      assertEdbPredicates(expected, "q15-rew.dlp")
    }

  }

  private def assertGoalPredicate(expected: Option[Predicate], ndlFileName: String) = {
    val ndl = ReWriter.getDatalogRewriting(s"$folderPathTest$ndlFileName")
    val actual = NdlUtils.getGoalPredicate(ndl)
    assert(actual === expected)
  }

  private def assertIdbPredicates(expected: Set[Predicate], ndlFileName: String) = {
    val ndl = ReWriter.getDatalogRewriting(s"$folderPathTest$ndlFileName")
    val actual = NdlUtils.getIdbPredicates(ndl)
    assert(actual -- expected isEmpty)
  }

  private def assertEdbPredicates(expected: Set[Predicate], ndlFileName: String) = {
    val ndl = ReWriter.getDatalogRewriting(s"$folderPathTest$ndlFileName")
    val actual = NdlUtils.getEdbPredicates(ndl)
    assert(actual -- expected isEmpty)
  }
}
