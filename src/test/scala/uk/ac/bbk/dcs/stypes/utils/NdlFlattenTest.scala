package uk.ac.bbk.dcs.stypes.utils

import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.ReWriter

  class NdlFlattenTest extends FunSpec {

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

    it("should substitute p5(x,y) in example-rew-q15-p5") {
      executeTest("example-rew-q15-p5", "p5")
    }

    it("should substitute p28(x,y) in example-rew-mono-s-p28") {
      executeTest("example-rew-mono-s-p28", "p28")
    }

    it("should substitute p28(x,y) in example-rew-mono-p28") {
      executeTest("example-rew-mono-p28", "p28")
    }

    it("should substitute p28(x,y) in example-rew-q15-p28") {
      executeTest("example-rew-q15-p28", "p28")
    }

    it("should substitute p7(x,y) in example-rew-mono-p7") {
      executeTest("example-rew-mono-p7", "p7")
    }

    it("should substitute p7(x,y) in example-rew-q15-p7") {
      executeTest("example-rew-q15-p7", "p7")
    }

    it("should substitute p3(x,y) in example-rew-only-p3") {
      executeTest("example-rew-only-p3", "p3")
    }

    it("should substitute p3(x,y) in example-rew-q15-p3") {
      executeTest("example-rew-q15-p3", "p3")
    }

    it("should substitute p15(x0,y2) in example-rew-thesis-01-p15") {
      executeTest("example-rew-thesis-01-p15", "p15")
    }

    it("should substitute p14(x7,y5) in example-rew-thesis-01-p14") {
      executeTest("example-rew-thesis-01-p14", "p14")
    }

  }

  private def executeTest(fileTest:String, predicateIdentifier: String, printLog:Boolean = false) = {
    val folderPathTest = "src/test/resources/rewriting/substitution/"
    val ndlExpected = ReWriter.getDatalogRewriting(s"$folderPathTest$fileTest-res.dlp")
    val ndl = ReWriter.getDatalogRewriting(s"$folderPathTest$fileTest.dlp")
    val actual = NdlFlatten.idbPredicateFlatten(ndl, predicateIdentifier)

    if ( printLog ) {
      println(s"begin substituting file test: $fileTest")
      println("ndl")
      ndl.foreach(println(_))
      println("actual")
      actual.foreach(println(_))
      println("expected")
      ndlExpected.foreach(println(_))
      println(s"end substituting file test: $fileTest")
    }

    assert(actual === ndlExpected)
  }
}
