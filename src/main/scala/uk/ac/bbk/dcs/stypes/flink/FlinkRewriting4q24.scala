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
  * uk.ac.bbk.dcs.stypes.flink.FlinkRewriting4q24
  */
object FlinkRewriting4q24 extends BaseFlinkRewriting {

  def main(args: Array[String]): Unit = {
    if (args.length > 1)
      FlinkRewriting4q24.run(args(0).toInt, args(1))
    else
      FlinkRewriting4q24.run(args(0).toInt)
  }

  def run(fileNumber: Int, serial: String = UUID.randomUUID().toString): Unit = {
    execute(fileNumber, serial, "q24", rewritingEvaluation)
  }

  def rewritingEvaluation(fileNumber: Int): DataSet[(String, String)] = {

    val a: DataSet[(String, String)] = getA(fileNumber)
    val b: DataSet[(String, String)] = getB(fileNumber)
    val r: DataSet[(String, String)] = getR(fileNumber)
    val s: DataSet[(String, String)] = getS(fileNumber)

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

    p1

  }

}
