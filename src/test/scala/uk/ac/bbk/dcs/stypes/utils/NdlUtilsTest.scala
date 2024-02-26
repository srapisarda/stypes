package uk.ac.bbk.dcs.stypes.utils

import fr.lirmm.graphik.graal.api.core.Predicate
import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.ReWriter

import scala.language.postfixOps

class NdlUtilsTest extends FunSpec {
  val folderPathTest = "src/test/resources/rewriting/"

  describe("Ndl Utils: get goal predicate") {

    it("should find the GOL predicate in q01-rew_test.dlp") {
      val expected = new Predicate("p1", 2)
      assertGoalPredicate(expected, "q01-rew_test.dlp")
    }

    it("should find the GOL predicate in q02-rew_test.dlp") {
      val expected = new Predicate("p1", 2)
      assertGoalPredicate(expected, "q02-rew_test.dlp")
    }

    it("should find the GOL predicate in q15-rew.dlp") {
      val expected = new Predicate("p1", 2)
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

  describe("Ndl Utils: get depth by Idb predicate") {
    it("should get the depth of q03-rew_test") {
      executeDepthTest("q03-rew_test", 2)
    }

    it("should get the depth of q07-rew_test") {
      executeDepthTest("q07-rew_test", 6)
    }

    it("should get the depth of q22-rew_test") {
      executeDepthTest("q22-rew_test", 3)
    }

  }

  describe("NDL Util: getIdbPredicatesDefCount") {
    it("should return correct number of idb declarations and the use of it in any clause body in q01-rew_test.dlp") {
      assertGetIdbPredicatesDefCount(expected = Map("p1" -> (1, 0)), ndlFileName = "q01-rew_test.dlp")
    }

    it("should return correct number of idb declarations and the use of it in any clause body in q03-rew_test.dlp") {
      assertGetIdbPredicatesDefCount(expected = Map("p2" -> (1, 1), "p1" -> (2, 0)), ndlFileName = "q03-rew_test.dlp")
    }

    it("should return correct number of idb declarations and the use of it in any clause body in q04-rew_test.dlp≈") {
      assertGetIdbPredicatesDefCount(expected = Map("p2" -> (1, 1), "p1" -> (1, 0), "p3" -> (1, 1)), ndlFileName = "q04-rew_test.dlp")
    }

    it("should return correct number of idb declarations and the use of it in any clause body in q07-rew_test.dlp") {
      assertGetIdbPredicatesDefCount(
        expected = Map("p12" -> (2, 1), "p3" -> (3, 2), "p2" -> (2, 1), "p9" -> (2, 2), "p1" -> (2, 0), "p6" -> (2, 2)),
        ndlFileName = "q07-rew_test.dlp")
    }

    it("should return correct number of idb declarations and the use of it in any clause body in q15-rew_test.dlp") {
      assertGetIdbPredicatesDefCount(
        expected = Map("p1" -> (3, 0), "p7" -> (2, 2), "p19" -> (2, 4), "p5" -> (2, 1), "p40" -> (2, 1), "p3" -> (3, 1), "p2" -> (2, 2),
          "p14" -> (2, 2), "p28" -> (2, 2), "p43" -> (3, 2), "p35" -> (3, 2)),
        ndlFileName = "q15-rew.dlp")
    }
  }

  describe("NDL thesis omq stats") {
    it("should count all the row and IDBs and EDBs") {
      val files = List("src/test/resources/rewriting/q15-rew.dlp",
        "src/test/resources/rewriting/q22-rew_test.dlp",
        "src/test/resources/rewriting/q45-rew_test.dlp")

      val ndlInfos = files.map(filename => {
        val ndl = ReWriter.getDatalogRewriting(filename = filename)
        val ndlInfo = NdlUtils.getNldInfo(ndl)
        assert(ndlInfo.edbSet.size == 4)
        (filename.split("/").last, ndlInfo)
      })

      println("omq,numCloses,numIdb,numEdb,avgClauseBodyLength,avgNumIDB,avgNumEDB")
      ndlInfos.foreach {
        case (filename, ndlInfo) =>
          println(s"$filename,${ndlInfo.numCloses},${ndlInfo.numIdb},${ndlInfo.numEdb},${ndlInfo.avgClauseBodyLength},${ndlInfo.avgNumIDB},${ndlInfo.avgNumEDB}")
      }
    }
  }

  private def executeDepthTest(fileTest: String, expected: Int): Unit = {
    val folderPathTest = "src/test/resources/rewriting/"
    val ndl = ReWriter.getDatalogRewriting(s"$folderPathTest$fileTest.dlp")
    val actual = NdlUtils.getDepthByIdbPredicate(ndl)
    assert(actual === expected)
  }

  private def assertGoalPredicate(expected: Predicate, ndlFileName: String) = {
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

  private def assertGetIdbPredicatesDefCount(expected: Map[String, (Int, Int)], ndlFileName: String) = {
    val ndl = ReWriter.getDatalogRewriting(s"$folderPathTest$ndlFileName")
    val actual = NdlUtils.getIdbPredicatesDefCount(ndl)
    assert(actual == expected)
  }


}
