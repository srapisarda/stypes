package uk.ac.bbk.dcs.stypes.flink

/*
 * #%L
 * stypes
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
  */
class FlinkOldQ15Test extends FunSpec with BaseFlinkTest {



  describe("Flink TEST3") {


    it("should read and execute the 'Bolzano' query rewrote for 1.ffl file set {A, B, R, S}") {
      execute(1, 2000)
    }

    ignore("should read and execute the 'Bolzano' query rewrote for 2.ffl file set {A, B, R, S}") {
      execute(2, 0)
    }

    ignore("should read and execute the 'Bolzano' query rewrote for 3.ffl file set {A, B, R, S}") {
      execute(3, 0)
    }

    ignore("should read and execute the 'Bolzano' query rewrote for 4.ffl file set {A, B, R, S}") {
      execute(4, 12165)
    }

    ignore("should read and execute the 'Bolzano' query rewrote for 5.ffl file set {A, B, R, S}") {
      execute(5, 46636)
    }

    def execute( fileNumber:Int,  expected:Int):Unit = {

      val a = getA(fileNumber)
      val b = getB(fileNumber)
      val r = getR(fileNumber)
      val s = getS(fileNumber)


      //<P-0-1> (?X,?Y) :- <R> (?X, ?Y) .
      lazy val p0_1 = r

      //<P-1-2> (?X,?Y) :- <R> (?X, ?Y) .
      lazy val p1_2 = r

      //<P-2-3> (?X,?Y) :- <S> (?X, ?Y) .
      lazy val p2_3 = s

      //<P-3-4> (?X,?Y) :- <R> (?X, ?Y) .
      lazy val p3_4 = r

      //<P-4-5> (?X,?Y) :- <S> (?X, ?Y) .
      lazy val p4_5 = s

      //<P-6-7> (?X,?Y) :- <S> (?X, ?Y) .
      lazy val p6_7 = s

      //<P-7-8> (?X,?Y) :- <R> (?X, ?Y) .
      lazy val p7_8 = r

      //<P-8-9> (?X,?Y) :- <R> (?X, ?Y) .
      lazy val p8_9 = r

      //<P-9-10> (?X,?Y) :- <S> (?X, ?Y) .
      lazy val p9_10 = s

      //<P-10-11> (?X,?Y) :- <R> (?X, ?Y) .
      lazy val p10_11 = r

      //<P-9-11> (?X,?X) :- <A>(?X).
      //<P-9-11> (?X,?Y) :- <P-9-10> (?X, ?Z),<P-10-11> (?Z, ?Y).
      lazy val  p9_11 = a.union(myJoin(p9_10, p10_11))

      //<P-1-3> (?X,?X) :- <B>(?X).
      //<P-1-3> (?X,?Y) :- <P-1-2> (?X, ?Z),<P-2-3> (?Z, ?Y).
      lazy val p1_3 = b.union( myJoin( p1_2 , p2_3 ) )

      //<P-0-3> (?X,?Y) :- <P-0-1> (?X, ?Z),<P-1-3> (?Z, ?Y).
      lazy val p0_3 = myJoin(p0_1, p1_3)

      //<P-3-5> (?X,?Y) :- <P-1-3> (?X, ?Y) .
      lazy val p3_5 = p1_3

      //<P-5-7> (?X,?Y) :- <P-1-3> (?X, ?Y) .
      lazy val p5_7 = p1_3

      //<P-3-7> (?X,?Y) :- <P-3-5> (?X, ?Z),<P-5-7> (?Z, ?Y).
      //<P-3-7> (?X,?Y) :- <P-3-4>(?X, ?Z), <A>(?Z),<P-6-7> (?Z, ?Y).
      lazy val p3_7 = myJoin(p3_5, p5_7).union(myJoin(myJoin(p3_4, a), p6_7))

      //<P-0-2> (?X,?Y) :- <P-0-1> (?X, ?Z),<P-1-2> (?Z, ?Y).
      lazy val p0_2 = myJoin(p0_1, p1_2)

      //<P-4-6> (?X,?Y) :- <P-9-11> (?X, ?Y) .
      lazy val p4_6 = p9_11

      //<P-4-7> (?X,?Y) :- <A>(?X), <P-6-7>(?X, ?Y) .
      //<P-4-7> (?X,?Y) :- <P-4-5> (?X, ?Z),<P-5-7> (?Z, ?Y).
      lazy val p4_7 = myJoin(a, p6_7).union( myJoin(p4_5, p5_7) )

      //<P-0-7> (?X,?Y) :- <P-0-3> (?X, ?Z),<P-3-7> (?Z, ?Y).
      //<P-0-7> (?X,?Y) :- <P-0-2>(?X, ?Z), <A>(?Z),<P-4-7> (?Z, ?Y).
      lazy val p0_7 = myJoin(p0_3, p3_7).union( myJoin(myJoin( p0_2, a), p4_7) )

      //<P-7-9> (?X,?Y) :- <P-0-2> (?X, ?Y) .
      lazy val p7_9 = p0_2

      //<P-7-11> (?X,?Y) :- <P-7-9> (?X, ?Z),<P-9-11> (?Z, ?Y).
      //<P-7-11> (?X,?Y) :- <P-7-8>(?X, ?Z), <B>(?Z),<P-10-11> (?Z, ?Y).
      lazy val p7_11 = myJoin(p7_9, p9_11).union( myJoin( myJoin( p7_8, b ), p10_11 ))

      //<P-8-11> (?X,?Y) :- <P-8-9> (?X, ?Z),<P-9-11> (?Z, ?Y).
      //<P-8-11> (?X,?Y) :- <B>(?X),<P-10-11>(?X, ?Y).
      lazy val p8_11 =  myJoin( p8_9, p9_11).union( myJoin(b, p10_11) )

      //<P-3-6> (?X,?Y) :- <P-8-11> (?X, ?Y) .
      lazy val p3_6 = p8_11

      //<P-13-15> (?X,?Y) :- <P-9-11> (?X, ?Y) .
      lazy val p13_15 = p9_11

      //<P-11-13> (?X,?Y) :- <P-1-3> (?X, ?Y) .
      lazy val p11_13 = p1_3

      //<P-11-15> (?X,?Y) :- <P-11-13> (?X, ?Z),<P-13-15> (?Z, ?Y).
      lazy val p11_15 =  myJoin(p11_13, p13_15)


      //<P-7-15> (?X,?Y) :- <P-7-11> (?X, ?Z),<P-11-15> (?Z, ?Y).
      lazy val p7_15 = myJoin( p7_11, p11_15 )

      //<P-8-15> (?X,?Y) :- <P-8-11> (?X, ?Z),<P-11-15> (?Z, ?Y).
      lazy val p8_15 = myJoin(p8_11, p11_15)

      //<P-0-6> (?X,?Y) :- <P-0-3> (?X, ?Z),<P-3-6> (?Z, ?Y).
      //<P-0-6> (?X,?Y) :- <P-0-2>(?X, ?Z), <A>(?Z),<P-4-6> (?Z, ?Y).
      lazy val p0_6 = myJoin(p0_3, p3_6).union(myJoin(myJoin(p0_2, a), p4_6))

      //<P-0-15> (?X,?Y) :- <P-0-7> (?X, ?Z),<P-7-15> (?Z, ?Y).
      //<P-0-15> (?X,?Y) :- <P-0-6>(?X, ?Z), <A>(?Z),<P-8-15> (?Z, ?Y).
      lazy val p0_15 = myJoin(p0_7, p7_15).union(
        myJoin( myJoin(p0_6, a), p8_15 ))

      val p0_15_distinct = p0_15.distinct()

      p0_15_distinct.writeAsCsv(s"$pathToBenchmarkNDL_SQL/data/rewriting-results-01-${new Date().getTime}")

      val count = p0_15_distinct.count
      println(s"p0_15.distinct.count: $count")

      assert( count == expected)
    }

  }

}
