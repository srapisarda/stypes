package uk.ac.bbk.dcs.stypes.utils

import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.ReWriter

  class NdlSubstitutionTest extends FunSpec {

  describe("IDB predicate substitution") {

    it("should substitute p(x) in example-rew-01") {
      executeTest( "example-rew-01", "p")
    }

    it("should substitute p(x) in example-rew-02") {
      executeTest( "example-rew-02", "p")
    }

    it("should substitute p3(x,y) in example-rew-q22-p3") {
      executeTest("example-rew-q22-p3", "p3")
    }

    it("should substitute p12(x,y) in example-rew-q22-p12") {
      executeTest("example-rew-q22-p12", "p12")
    }

    it("should substitute p5(x,y) in example-rew-mono-term") {
      executeTest("example-rew-mono-term", "p5")
    }

  }

  private def executeTest(fileTest:String, predicateIdentifier: String, printLog:Boolean = false) = {
    val folderPathTest = "src/test/resources/rewriting/substitution/"
    val ndlExpected = ReWriter.getDatalogRewriting(s"$folderPathTest$fileTest-res.dlp")
    val ndl = ReWriter.getDatalogRewriting(s"$folderPathTest$fileTest.dlp")
    val actual = NdlSubstitution.idbPredicateSubstitution(ndl, predicateIdentifier)

    if ( printLog ) {
      println(s"begin substituting file test: $fileTest")
      println("actual")
      actual.foreach(println(_))
      println("expected")
      ndlExpected.foreach(println(_))
      println(s"end substituting file test: $fileTest")
    }

    assert(actual === ndlExpected)
  }
}
