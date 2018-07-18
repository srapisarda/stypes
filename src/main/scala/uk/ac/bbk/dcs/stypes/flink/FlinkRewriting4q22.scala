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
  * Created by salvo on 10/05/2018.
  *
  * uk.ac.bbk.dcs.stypes.flink.FlinkRewriting4q22
  */
object FlinkRewriting4q22 extends BaseFlinkRewriting {

  def main(args: Array[String]): Unit = {
    if (args.length > 1)
      FlinkRewriting4q22.run(args(0).toInt, args(1))
    else
      FlinkRewriting4q22.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    execute(fileNumber, serial, "q22", rewritingEvaluation)
  }

  def rewritingEvaluation(fileNumber: Int): DataSet[(String, String)] = {

    val a: DataSet[(String, String)] = getA(fileNumber)
    val b: DataSet[(String, String)] = getB(fileNumber)
    val r: DataSet[(String, String)] = getR(fileNumber)
    val s: DataSet[(String, String)] = getS(fileNumber)


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


    p1

  }

}
