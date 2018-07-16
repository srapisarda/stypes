package uk.ac.bbk.dcs.stypes.flink

/*
 * #%L
 * stype
 * %%
 * Copyright (C) 2017 - 2021 Birkbeck University of London
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.Date

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.scalatest.FunSpec

/**
  * Created by salvo on 01/01/2018.
  *
  * Flink test based on the rewriting for query q15.cq  using  lines.dlp
  *
  * p1(x0,x7) :- r(x3,x4), p12(x7,x4), p3(x0,x3).
  * p3(x0,x3) :- r(x0,x3), a(x0).
  * p3(x0,x3) :- r(x1,x2), r(x2,x3), s(x0,x1).
  * p12(x7,x4) :- r(x5,x6), r(x4,x5), s(x6,x7).
  * p12(x7,x4) :- r(x4,x7), b(x7).
  *
  */
class FlinkQ22Test extends FunSpec with BaseFlinkTest{

  describe("Flink q22") {

    it("should read and execute the 'q22.cq' query rewrote for 1.ffl file set {A, B, R, S}") {
      execute(1, 2832)
    }

    it("should read and execute the 'q22.cq' query rewrote for 2.ffl file set {A, B, R, S}") {
      execute(2, 248)
    }

    it("should read and execute the 'q22.cq' query rewrote for 3.ffl file set {A, B, R, S}") {
      execute(3, 2125)
    }

    ignore("should read and execute the 'q22.cq' query rewrote for 4.ffl file set {A, B, R, S}") {
      execute(4, 62572)
    }

    ignore("should read and execute the 'q22.cq' query rewrote for 5.ffl file set {A, B, R, S}") {
      execute(5, 119951)
    }

    ignore("should read and execute the 'q22.cq' query rewrote for 6.ffl file set {A, B, R, S}") {
      execute(6, 105467)
    }

    def execute(fileNumber: Int, expected: Int): Unit = {

      val a = getA(fileNumber)
      val b = getB(fileNumber)
      val r = getR(fileNumber)
      val s = getS(fileNumber)


      // p1(x0,x7) :- p3(x0,x3), r(x3,x4), p12(x7,x4).
      lazy val p1 = myJoin(myJoin(p3, r), switchTerms(p12))

      // p3(x0,x3) :-  a(x0), r(x0,x3).
      // p3(x0,x3) :- s(x0,x1), r(x1,x2), r(x2,x3).
      lazy val p3 = myJoin(a, r)
        .union(myJoin(myJoin(s, r), r))

      // p12(x7,x4) :-  r(x4,x5), r(x5,x6), s(x6,x7).
      // p12(x7,x4) :- r(x4,x7), b(x7).
      lazy val p12 = switchTerms(myJoin(myJoin(r, r), s)
          .union(myJoin(r, b)))


      val p1_distinct = p1.distinct()

      p1_distinct.writeAsCsv(s"$pathToBenchmarkNDL_SQL/data/rewriting-results-p22-ttl_${fileNumber}-${new Date().getTime}")

      val count = p1_distinct.count
      println(s"p1_22.distinct.count: $count")

      assert(count == expected)
    }

  }

}
