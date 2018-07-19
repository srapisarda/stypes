package uk.ac.bbk.dcs.stypes.flink

/*
 * #%L
 * STypeS
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
import org.scalatest.FunSpec

/**
  * Created by salvo on 01/01/2018.
  *
  * Flink test based on the rewriting for query q24.cq  using  lines.dlp
  *
  */
class FlinkQ24Test extends FunSpec with BaseFlinkTest{

  describe("Flink q24") {

    it("should read and execute the 'q24.cq' query rewrote for 1.ffl file set {A, B, R, S}") {
      execute(1, 2832 )
    }

    it("should read and execute the 'q24.cq' query rewrote for 2.ffl file set {A, B, R, S}") {
      execute(2, 248)
    }

    it("should read and execute the 'q24.cq' query rewrote for 3.ffl file set {A, B, R, S}") {
      execute(3, 2125)
    }

    ignore("should read and execute the 'q24.cq' query rewrote for 4.ffl file set {A, B, R, S}") {
      execute(4, 62572)
    }

    ignore("should read and execute the 'q24.cq' query rewrote for 5.ffl file set {A, B, R, S}") {
      execute(5, 119951)
    }

    ignore("should read and execute the 'q24.cq' query rewrote for 6.ffl file set {A, B, R, S}") {
      execute(6, 105467)
    }

    def execute(fileNumber: Int, expected: Int): Unit = {

      val a = getA(fileNumber)
      val b = getB(fileNumber)
      val r = getR(fileNumber)
      val s = getS(fileNumber)

      // p17(x9,x5) :- b(x5), p11(x5,x9).
      // p17(x9,x5) :- r(x5,x8), a(x8), s(x8,x9).
      // p17(x9,x5) :-  r(x5,x6), s(x6,x7), p11(x7,x9).
      lazy val p17_switched =  myJoin(b, p11)
        .union (  myJoin( myJoin( r, a ), s ))
        .union(  myJoin(myJoin( r, s ), p11 ) )

      // p11(x9,x9) :- b(x9).
      // p11(x7,x9) :- r(x7,x8), s(x8,x9).
      lazy val p11 = b.
        union( myJoin( r, s ) )

      // p5(x0,x2) :- s(x0,x1), r(x1,x2).
      // p5(x0,x0) :- a(x0).
      lazy val p5 = myJoin( s, r)
        .union(a)

      // p1(x0,x9) :-  p5(x0,x2), r(x2,x3), r(x3,x4), r(x4,x5), p17(x9,x5).
      lazy val p1 = myJoin( myJoin(myJoin( myJoin( p5, r ), r) ,r ), p17_switched)

      val p1_distinct = p1.distinct()

      p1_distinct.writeAsCsv(s"$pathToBenchmarkNDL_SQL/data/rewriting-results-q24-ttl_${fileNumber}-${new Date().getTime}")

      val count = p1_distinct.count
      println(s"p1_24.distinct.count: $count")

      assert(count == expected)
    }

  }

}
