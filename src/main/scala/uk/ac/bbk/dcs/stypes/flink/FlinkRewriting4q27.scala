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
object FlinkRewriting4q27 extends BaseFlinkRewriting{

  def main(args: Array[String]): Unit = {
      if ( args.length > 1 )
        FlinkRewriting4q27.run(args(0).toInt, args(1))
      else
        FlinkRewriting4q27.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    execute(fileNumber, serial, "q27", rewritingEvaluation)
  }

  def rewritingEvaluation(fileNumber: Int): DataSet[(String, String)] = {

    val a: DataSet[(String, String)] = getA(fileNumber)
    val b: DataSet[(String, String)] = getB(fileNumber)
    val r: DataSet[(String, String)] = getR(fileNumber)
    val s: DataSet[(String, String)] = getS(fileNumber)

    val  r_join = myJoin(r,r)

    val r_double_join = myJoin(r_join,r)
    // p32(x7,x9) :- r(x7,x8), s(x8,x9).
    // p32(x9,x9) :- b(x9).
    val p32= myJoin(r,s ).union(b)

    // p27(x12,x7) :- r(x7,x8), a(x8), r(x8,x11), r(x11,x12).
    // p27(x12,x7) :- p32(x7,x9), r(x9,x10), r(x10,x11), r(x11,x12).
    val p27_switched =  myJoin(myJoin(r, a), r_join)
                    .union( myJoin(p32, r_double_join))

    // p5(x0,x3) :-  a(x0), r(x0,x3).
    // p5(x0,x3) :- s(x0,x1), r(x1,x2), r(x2,x3) .
    val p5 = myJoin(a, r)
        .union(  myJoin(s, r_join) )

    // p3(x12,x8) :- a(x8), s(x8,x9), r(x9,x10),r(x10,x11), r(x11,x12).
    val p3_1 = switchTerms(myJoin( myJoin( a, s ), r_double_join))
    // p3(x12,x8) :- a(x8), r(x8,x11), r(x11,x12).
    val p3_2 = myJoin( a, r_join)

    val p3 = p3_1 union p3_2

    val p5_r_join = myJoin(p5, r_join)

    //val p27_switched = switchTerms(p27)
    // p1(x0,x12) :- p5(x0,x3), r(x3,x4), r(x4,x5), r(x5,x6),  a(x6), p3(x12,x6).
    val p1_1 = myJoin(myJoin(myJoin(p5, r_double_join), a), switchTerms(p3))
    // p1(x0,x12) :- p5(x0,x3), r(x3,x4), r(x4,x5), b(x5), p27(x12,x5).
    val p1_2 = myJoin(myJoin(p5_r_join, b), p27_switched)
    // p1(x0,x12) :- p5(x0,x3), r(x3,x4), r(x4,x5), r(x5,x6), s(x6,x7), p27(x12,x7).
    val p1_3 = myJoin(myJoin(p5_r_join, s), p27_switched)

    val p1 =  p1_1 union p1_2 union p1_3

    p1
  }


  case class QueryEvaluation( ttl: Int, parallelism: Int, milliseconds:Long, count:Long, resultPath:String    )

}
