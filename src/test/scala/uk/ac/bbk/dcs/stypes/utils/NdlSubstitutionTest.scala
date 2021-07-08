package uk.ac.bbk.dcs.stypes.utils

import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.ReWriter

class NdlSubstitutionTest extends FunSpec {

  describe("IDB predicate substitution") {

    it("should substitute p(x) in example-rew-01") {
      executeTest( "example-rew-01")
    }

    it("should substitute p(x) in example-rew-02") {
      executeTest( "example-rew-02")
    }

  }

  private def executeTest(fileTest:String) = {
    val folderPathTest = "src/test/resources/rewriting/substitution/"
    val ndlExpected = ReWriter.getDatalogRewriting(s"$folderPathTest$fileTest-res.dlp")
    val ndl = ReWriter.getDatalogRewriting(s"$folderPathTest$fileTest.dlp")
    val actual = NdlSubstitution.idbPredicateSubstitution(ndl, "p")

    assert(actual === ndlExpected)
  }
}
