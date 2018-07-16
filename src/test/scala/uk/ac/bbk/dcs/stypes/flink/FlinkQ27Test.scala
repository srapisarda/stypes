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
class FlinkQ27Test extends FunSpec with BaseFlinkTest{

  describe("Flink q27") {


    ignore("should read and execute the 'q27.cq' query rewrote for 1.ffl file set {A, B, R, S}") {
      execute(1, 2832)
    }

    ignore("should read and execute the 'q27.cq' query rewrote for 2.ffl file set {A, B, R, S}") {
      execute(2, 248)
    }

    ignore("should read and execute the 'q27.cq' query rewrote for 3.ffl file set {A, B, R, S}") {
      execute(3, 2125)
    }

    ignore("should read and execute the 'q27.cq' query rewrote for 4.ffl file set {A, B, R, S}") {
      execute(4, 62572)
    }

    ignore("should read and execute the 'q27.cq' query rewrote for 5.ffl file set {A, B, R, S}") {
      execute(5, 119951)
    }

    ignore("should read and execute the 'q27.cq' query rewrote for 6.ffl file set {A, B, R, S}") {
      execute(6, 105467)
    }

    def execute(fileNumber: Int, expected: Int): Unit = {

      val a = getA(fileNumber)
      val b = getB(fileNumber)
      val r = getR(fileNumber)
      val s = getS(fileNumber)


      val  r_join = myJoin(r,r)

      lazy val r_double_join = myJoin(r_join,r)
      // p32(x7,x9) :- r(x7,x8), s(x8,x9).
      // p32(x9,x9) :- b(x9).
      lazy val p32= myJoin(r,s ).union(b)

      // p27(x12,x7) :- r(x7,x8), a(x8), r(x8,x11), r(x11,x12).
      // p27(x12,x7) :- p32(x7,x9), r(x9,x10), r(x10,x11), r(x11,x12).
      lazy val p27_switched =  myJoin(myJoin(r, a), r_join)
        .union( myJoin(p32, r_double_join))

      // p5(x0,x3) :-  a(x0), r(x0,x3).
      // p5(x0,x3) :- s(x0,x1), r(x1,x2), r(x2,x3) .
      lazy val p5 = myJoin(a, r)
        .union(  myJoin(s, r_join) )

      // p3(x12,x8) :- a(x8), s(x8,x9), r(x9,x10),r(x10,x11), r(x11,x12).
      lazy val p3_1 = switchTerms(myJoin( myJoin( a, s ), r_double_join))
      // p3(x12,x8) :- a(x8), r(x8,x11), r(x11,x12).
      lazy val p3_2 = myJoin( a, r_join)

      lazy val p3 = p3_1 union p3_2

      lazy val p5_r_join = myJoin(p5, r_join)

      //lazy val p27_switched = switchTerms(p27)
      // p1(x0,x12) :- p5(x0,x3), r(x3,x4), r(x4,x5), r(x5,x6),  a(x6), p3(x12,x6).
      lazy val p1_1 = myJoin(myJoin(myJoin(p5, r_double_join), a), switchTerms(p3))
      // p1(x0,x12) :- p5(x0,x3), r(x3,x4), r(x4,x5), b(x5), p27(x12,x5).
      lazy val p1_2 = myJoin(myJoin(p5_r_join, b), p27_switched)
      // p1(x0,x12) :- p5(x0,x3), r(x3,x4), r(x4,x5), r(x5,x6), s(x6,x7), p27(x12,x7).
      lazy val p1_3 = myJoin(myJoin(p5_r_join, s), p27_switched)

      lazy val p1 =  p1_1 union p1_2 union p1_3

      //lazy val p1_distinct =  p1.distinct()

      lazy val p1_distinct =  p1_1.distinct()


      p1_distinct.writeAsCsv(s"$pathToBenchmarkNDL_SQL/data/rewriting-results-q27-ttl_${fileNumber}-${new Date().getTime}")

      lazy val count = p1_distinct.count
      println(s"p1_27.distinct.count: $count")

      assert(count == expected)
    }

  }

}
