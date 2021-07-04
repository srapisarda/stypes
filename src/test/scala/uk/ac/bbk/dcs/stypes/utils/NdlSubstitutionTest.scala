package uk.ac.bbk.dcs.stypes.utils

import org.scalatest.FunSpec
import uk.ac.bbk.dcs.stypes.ReWriter

class NdlSubstitutionTest  extends FunSpec {
  describe("IDB predicate substitution") {

    it("should substitute p(x) in example-rew-01") {
      val fileTest = "example-rew-01"

      val ndlExpected = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/substitution/$fileTest-res.dlp")
      val ndl = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/substitution/$fileTest.dlp")
      val actual = NdlSubstitution.idbPredicateSubstitution(ndl, "p")

      assert(actual === ndlExpected)
    }
  }

}
