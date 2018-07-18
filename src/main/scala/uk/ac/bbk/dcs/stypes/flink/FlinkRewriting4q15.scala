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

import java.util.UUID
import org.apache.flink.api.scala._

/**
  * Created by Salvatore Rapisarda on 10/05/2018.
  *
  * uk.ac.bbk.dcs.stypes.flink.FlinkRewriting4q15
  */
object FlinkRewriting4q15 extends BaseFlinkRewriting {

  def main(args: Array[String]): Unit = {
    if (args.length > 1)
      FlinkRewriting4q15.run(args(0).toInt, args(1))
    else
      FlinkRewriting4q15.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    execute(fileNumber, serial, "q15", rewritingEvaluation)
  }

  def rewritingEvaluation(fileNumber: Int): DataSet[(String, String)] = {

    val a: DataSet[(String, String)] = getA(fileNumber)
    val b: DataSet[(String, String)] = getB(fileNumber)
    val r: DataSet[(String, String)] = getR(fileNumber)
    val s: DataSet[(String, String)] = getS(fileNumber)

    // p40(x7,x5) :- b(x5), r(x5,x6), s(x6,x7).
    // p40(x5,x5) :- b(x5).
    lazy val p40 = myJoin(b, myJoin(r, s)).map(p => (p._2, p._1)).union(b)

    // p43(x7,x4) :- s(x4,x5), r(x5,x6), s(x6,x7).
    // p43(x7,x4) :- a(x4), s(x4,x7).
    // p43(x7,x4) :- s(x4,x7), b(x7).
    lazy val p43 = myJoin(s, myJoin(r, s)).map(p => (p._2, p._1))
      .union(myJoin(a, s)).map(p => (p._2, p._1))
      .union(myJoin(s, b)).map(p => (p._2, p._1))


    // p19(x0,x3) :- r(x0,x3), b(x3).
    // p19(x0,x3) :- r(x0,x1), r(x1,x2), s(x2,x3) .
    lazy val p19 = myJoin(r, b)
      .union(myJoin(myJoin(r, r), s))

    // p35(x0,x7) :- p43(x7,x2), r(x0,x1), r(x1,x2), a(x2).
    // p35(x0,x7) :- p19(x0,x3), r(x3,x4), p43(x7,x4) .
    // p35(x0,x7) :- p40(x7,x3), p19(x0,x3),  b(x3).
    lazy val p35 = myJoin(p43, myJoin(r, myJoin(r, a)).map(p => (p._2, p._1))).map(p => (p._2, p._1))
      .union(myJoin(myJoin(p19, r), p43.map(p => (p._2, p._1))))
      .union(switchTerms(myJoin(p40, switchTerms(myJoin(p19, b)))))

    // p5(x8,x8) :- b(x8).
    // p5(x10,x8) :- s(x9,x10), r(x8,x9).
    lazy val p5 = b.union(myJoin(switchTerms(s), switchTerms(r)))

    // p7(x15,x15) :- a(x15).
    // p7(x13,x15) :- s(x13,x14), r(x14,x15).
    lazy val p7 = a.union(myJoin(s, r))


    // p14(x11,x15) :- r(x11,x12), s(x12,x13), p7(x13,x15).
    // p14(x11,x15) :- b(x11), p7(x11,x15).
    lazy val p14 = myJoin(r, myJoin(s, p7))
      .union(myJoin(b, p7))


    // p2(x8,x15) :- p5(x10,x8), r(x10,x11), p14(x11,x15).
    // p2(x8,x15) :- r(x8,x9), a(x9), p14(x9,x15).
    lazy val p2 = myJoin(switchTerms(p5), myJoin(r, p14))
      .union(myJoin(r, myJoin(a, p14)))

    // p28(x4,x4) :- a(x4).
    // p28(x4,x6) :- s(x4,x5), r(x5,x6), a(x6).
    lazy val p28 = a.union(myJoin(s, myJoin(r, a)))


    // p3(x0,X) :- p19(x0,x3), r(x3,x4), p28(x4,X).
    // p3(x0,x2) :- r(x0,x1), r(x1,x2), a(x2), p28(x2,x2).
    // p3(x0,x6) :-  p19(x0,x6), b(x6), a(x6), r(x6,x6).
    lazy val p3 = myJoin(p19, myJoin(r, p28))
      .union(myJoin(r, myJoin(r, myJoin(a, p28))))
      .union(myJoin(p19, myJoin(b, myJoin(a, r))))

    lazy val p34 = unknownData2

    // p1(x0,x15) :- p35(x0,x7), r(x7,x8), p2(x8,x15) .
    // p1(x0,x15) :- p3(x0,x8), a(x8), p2(x8,x15).
    // p1(x0,x15) :- p35(x0,x7),  b(x7), p34(x15,x7),.
    lazy val p1 = myJoin(p35, myJoin(r, p2))
      .union(myJoin(p3, myJoin(a, p2)))
      .union(myJoin(switchTerms(p34), b))

    p1

  }


}
