package uk.ac.bbk.dcs.stypes.optimise

import org.scalatest.FunSuite
import uk.ac.bbk.dcs.stypes.{Clause, ReWriter}

class SubExpressionSubstitutionTest extends FunSuite {

  test("it should find the two sub expressions in the clauses body") {
    val datalog: List[Clause] = ReWriter.getDatalogRewriting("src/test/resources/rewriting/optimise/sub-expression-substitution-test-01.dlp")
    println(s"datalog to optimise:\n${datalog.mkString("\n")}")

    val optimised = SubExpressionSubstitution.optimise(datalog, CatalogStatistics(Map()))
    println(s"\ndatalog to optimised by sub-expression substitution:\n${optimised.mkString("\n")}")

    val comp = optimised.map(_.head).find(_.getPredicate.getIdentifier.equals("comp_0"))
    assert(comp.isDefined)
  }
}
