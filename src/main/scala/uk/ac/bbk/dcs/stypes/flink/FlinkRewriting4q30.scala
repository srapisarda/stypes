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
  * uk.ac.bbk.dcs.stypes.flink.FlinkRewriting4q30
  */
object FlinkRewriting4q30 extends BaseFlinkRewriting {

  def main(args: Array[String]): Unit = {
    if (args.length > 1)
      FlinkRewriting4q30.run(args(0).toInt, args(1))
    else
      FlinkRewriting4q30.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    execute(fileNumber, serial, "q30", rewritingEvaluation)
  }

  def rewritingEvaluation(fileNumber: Int): DataSet[(String, String)] = {

    val a: DataSet[(String, String)] = getA(fileNumber)
    val b: DataSet[(String, String)] = getB(fileNumber)
    val r: DataSet[(String, String)] = getR(fileNumber)
    val s: DataSet[(String, String)] = getS(fileNumber)

    // p14(x7,x4) :- r(x4,x7), b(x7).
    // p14(x7,x4) :- r(x4,x5), r(x5,x6), s(x6,x7).
    lazy val p14 = switchTerms(myJoin(r, b))
      .union(switchTerms(myJoin(myJoin(r, r), s)))

    // p45(x11,x8) :- s(x8,x9), r(x9,x10),  r(x10,x11).
    // p45(x11,x8) :- a(x8), r(x8,x11).
    lazy val p45 = switchTerms(myJoin(myJoin(s, r), r))
      .union(switchTerms(myJoin(a, r)))

    // p5(x0,x3) :- s(x0,x1), r(x1,x2), r(x2,x3).
    // p5(x0,x3) :- a(x0), r(x0,x3).
    lazy val p5 = myJoin(myJoin(s, r), r)
      .union(myJoin(a, r))

    // p1(x0,x15) :- p5(x0,x3), r(x3,x4), p14(x9,x4), b(x9), r(x9,x10), r(x10,x11), r(x11,x12),  r(x12,x13), r(x13,x14), r(x14,x15).
    // p1(x0,x15) :- p5(x0,x3), r(x3,x4), p14(x7,x4), r(x7,x8),   p45(x11,x8),  r(x11,x12), r(x12,x13), r(x13,x14), r(x14,x15).
    // p1(x0,x15) :- p5(x0,x3), r(x3,x4), r(x4,x5),  r(x5,x6), a(x6), p45(x11,x6), r(x11,x12), r(x12,x13), r(x13,x14), r(x14,x15).
    lazy val p1 = myJoin(myJoin(myJoin(myJoin(myJoin(myJoin(myJoin(myJoin(myJoin(p5, r), switchTerms(p14)), b), r), r), r), r), r), r)
      .union(myJoin(myJoin(myJoin(myJoin(myJoin(myJoin(myJoin(myJoin(p5, r), switchTerms(p14)), r), switchTerms(p45)), r), r), r), r))
      .union(myJoin(myJoin(myJoin(myJoin(myJoin(myJoin(myJoin(myJoin(myJoin(p5, r), r), r), a), switchTerms(p45)), r), r), r), r))


    val p1_distinct = p1.distinct()

    p1
  }

}
