package uk.ac.bbk.dcs.stypes.optimise

import org.scalatest.FunSuite
import uk.ac.bbk.dcs.stypes.{Clause, ReWriter}

import scala.io.Source

class SubExpressionSubstitutionTest extends FunSuite {

  test("it should find the two sub expressions in the clauses body") {
    executeTest("src/test/resources/rewriting/optimise/sub-expression-substitution-test-01")
  }

  test("it should find the two sub expressions in the clauses body with different variables") {
    executeTest("src/test/resources/rewriting/optimise/sub-expression-substitution-test-02")
  }

  test("it should find the two sub expressions in the clauses body with different atoms and variables") {
    executeTest("src/test/resources/rewriting/optimise/sub-expression-substitution-test-03")
  }

  private def executeTest(fileName: String): Unit = {
    val datalog: List[Clause] = ReWriter.getDatalogRewriting(s"$fileName.dlp")
    println(s"datalog to optimise:\n${datalog.mkString("\n")}")

    val optimised = SubExpressionSubstitution.optimise(datalog, CatalogStatistics(Map()))
    val actual = optimised.mkString("\n")
    println(s"\ndatalog to optimised by sub-expression substistion:\n$actual")

    val source = Source.fromFile(s"$fileName-res.txt")
    val expected = source.getLines().mkString("\n")
    source.close()
    assert(expected == actual)
  }
}
