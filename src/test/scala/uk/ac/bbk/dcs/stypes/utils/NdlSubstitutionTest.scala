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

    it("should sobstitute p3(x,y) in example-rew-q22-p3") {
      executeTest("example-rew-q22-p3", "p3")
    }

    it("should sobstitute p3(x,y) in example-rew-q22-p12") {
      executeTest("example-rew-q22-p12", "p12")
    }
  }

  private def executeTest(fileTest:String, predicateIdentifier: String) = {
    val folderPathTest = "src/test/resources/rewriting/substitution/"
    val ndlExpected = ReWriter.getDatalogRewriting(s"$folderPathTest$fileTest-res.dlp")
    val ndl = ReWriter.getDatalogRewriting(s"$folderPathTest$fileTest.dlp")
    val actual = NdlSubstitution.idbPredicateSubstitution(ndl, predicateIdentifier)

    assert(actual === ndlExpected)
  }
}
