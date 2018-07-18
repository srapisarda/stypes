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

import java.util.{Date, UUID}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

/**
  * Created by salvo on 10/05/2018.
  *
  * uk.ac.bbk.dcs.stypes.flink.FlinkRewriting4q45
  */
object FlinkRewriting4q45 extends BaseFlinkRewriting{

  def main(args: Array[String]): Unit = {
    if ( args.length > 1 )
      FlinkRewriting4q45.run(args(0).toInt, args(1))
    else
      FlinkRewriting4q45.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    execute(fileNumber, serial, "q45", rewritingEvaluation)
  }

  def rewritingEvaluation(fileNumber: Int): DataSet[(String, String)] = {

    val a: DataSet[(String, String)] = getA(fileNumber)
    val b: DataSet[(String, String)] = getB(fileNumber)
    val r: DataSet[(String, String)] = getR(fileNumber)
    val s: DataSet[(String, String)] = getS(fileNumber)


    // p14(x7,x4) :- a(x4), s(x4,x7).
    // p14(x7,x4) :- s(x4,x5), r(x5,x6), s(x6,x7).
    // p14(x7,x4) :- s(x4,x7), b(x7).
    lazy val p14 = myJoin(a, switchTerms(s))
      .union(switchTerms(myJoin(myJoin(s,r), s)))
      .union(switchTerms(myJoin(s, b)))

    // p15(x0,x0) :- b(x0), a(x0).
    // p15(x0,x2) :- s(x0,x1), r(x1,x2), b(x2).
    lazy val p15 = myJoin(b, a)
      .union(myJoin(myJoin(s, r),b))

    // p1(x0,x15) :-  p2(x0,x7), b(x7), p3(x15,x7).
    // p1(x0,x15) :-  p2(x0,x7), r(x7,x8), p37(x8,x15).
    // p1(x0,x15) :-  p36(x0,x8), a(x8),p37(x8,x15).
    lazy val p1 = myJoin(myJoin(p2, b), switchTerms(p15))
      .union(myJoin(myJoin(p2, r), p37))
      .union(myJoin(myJoin(p36, a), p37))

    // p21(x15,x13) :- a(x13), r(x13,x14), s(x14,x15).
    // p21(x15,x15) :- a(x15), b(x15).
    lazy val p21 = switchTerms(myJoin(myJoin(a, r), s))
      .union(myJoin(a, b))

    // p2(x0,x7) :- p15(x0,x4), b(x4), p14(x7,x4).
    // p2(x0,x7) :- p5(x0,x3), s(x3,x4), p14(x7,x4).
    lazy val p2 = myJoin(myJoin(p15, b), switchTerms(p14))
      .union(myJoin(myJoin(p5, s), switchTerms(p14)))

    // p31(x12,x15) :- r(x12,x13), r(x13,x14), s(x14,x15).
    // p31(x12,x15) :- r(x12,x15), b(x15).
    lazy val p31 = myJoin(myJoin(r, r), s)
      .union(myJoin(r, b))

    // p36(x0,X@x7) :- p5(x0,x3), s(x3,x4), p41(x4,X@x7).
    // p36(x0,X@x7) :- p15(x0,x4), b(x4), p41(x4,X@x7).
    lazy val p36 = myJoin(myJoin(p5, s), p41)
      .union(myJoin(myJoin(p15, b), p41))

    // p37(x8,x15) :- p42(x11,x8), a(x11), p21(x15,x11).
    // p37(x8,x15) :- p45(x8,x12), b(x12), p31(x12,x15).
    // p37(x8,x15) :- p42(x11,x8), s(x11,x12), p31(x12,x15).
    lazy val p37 = myJoin(myJoin(switchTerms(p42), a), switchTerms(p21))
      .union(myJoin(myJoin(p45, b), p31))
      .union(myJoin(myJoin(switchTerms(p42), s), p31))

    // p3(x15,x9) :- b(x9), r(x9,x10), b(x10), p31(x10,x15).
    // p3(x15,x9) :- b(x9), r(x9,x10), r(x10,x11), a(x11), p21(x15,x11).
    // p3(x15,x9) :- b(x9), r(x9,x10), r(x10,x11), s(x11,x12), p31(x12,x15).
    lazy val p3 = switchTerms(myJoin(myJoin(myJoin(b,r), b), p31))
      .union( switchTerms( myJoin(  myJoin(myJoin(myJoin( b,r ),r),a), switchTerms(p21))))
        .union( switchTerms( myJoin(myJoin(myJoin(myJoin(b,r),r),s ), p31)))

    // p41(x4,x4) :- a(x4).
    // p41(x4,x6) :- s(x4,x5), r(x5,x6), a(x6).
    lazy val p41 = a
      .union(myJoin( myJoin(s,r), a) )

    // p42(x11,x8) :- a(x8), r(x8,x11).
    // p42(x11,x8) :- s(x8,x9), r(x9,x10), r(x10,x11).
    lazy val p42 = switchTerms(myJoin(a,r))
      .union( switchTerms(myJoin(myJoin(s,r), r)))

    // p45(x8,x10) :- s(x8,x9), r(x9,x10), b(x10).
    // p45(x8,x8) :- a(x8), b(x8).
    lazy val p45 = myJoin(myJoin(s,r),b)
      .union(myJoin(a,b))

    // p5(x0,x3) :- a(x0), r(x0,x3) .
    // p5(x0,x3) :- s(x0,x1), r(x1,x2), r(x2,x3).
    lazy val p5 = myJoin(a,r)
      .union( myJoin(myJoin(s,r), r))

    p1

  }

}
