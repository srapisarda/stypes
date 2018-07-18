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

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * This object test is based on the following re-writer:
  * p6(X0) :- m139004(X0,X1,X2,X3).
  * p1() :- p6(X0), v763(X0,Y2,X8,X12).
  * p6(X0) :- v197(X0,X0,X2,X14).
  * p6(X0) :- v635(X8,X0,X1,X14).
  * p1() :- p6(X0), v199(X0,X0,Y1,X11).
  * p6(X0) :- v762(X0,X2,X8,X3).
  * p1() :- p6(X0), v174(X5,X7,X8,X0).
  * p6(X1) :- v466(X1,X6,X1,X9).
  * p6(X0) :- v308(X0,X12,X13,X14).
  * p6(X0) :- m56004(X1,X0,X7,X8).
  * p1() :- p6(X0), v760(X1,X0,X7,X8).
  * p1() :- m249004(X0,Y1,Y2,Y3), p6(X0).
  * p6(X0) :- v653(X0,X0,X1,X4).
  * p1() :- p6(X0), v708(X5,X0,X7,X4).
  * p1() :- p6(X0), v515(X0,Y2,X13,X14).
  * p1() :- p6(X0), v748(X0,X5,X6,Y1).
  * p6(X0) :- v51(X0,X4,X11,X14).
  * p6(X0) :- v781(X0,X5,X0,X7).
  * p6(X0) :- v22(X0,X0,X7,X14).
  * p1() :- p6(X0), v14(X0,X6,Y1,X14).
  * p6(X0) :- v905(X6,X0,X9,X10).
  * p1() :- p6(X0), v929(X0,X2,X0,Y1).
  *
  * The object class can be automatically produced by the method  generateFlinkScript the class ReWriter contains
  */
object FlinkRewriterTest extends App {

  val conf = new Configuration()
  conf.setInteger("taskmanager.numberOfTaskSlots",4)
  private val env = ExecutionEnvironment.createLocalEnvironment(conf)
  env.setParallelism(4)

  private  def  stringMapper1: (String) => (String) = (p: String) => {
    val line = p.split(',')
    line.head
  }

  private  def  stringMapper: (String) => (String, String) = (p: String) => {
    val line = p.split(',')
    (line.head, line.last)
  }

  private  def  stringMapper3: (String) => (String, String, String) = (p: String) => {
    val line = p.split(',')
    (line(0), line(1), line(2))
  }

  private  def  stringMapper4: (String) => (String, String, String, String) = (p: String) => {
    val line = p.split(',')
    (line(0), line(1), line(2), line(3))
  }

  private  def  unknownData1 ={
    val ds: DataSet[(String)] =  env.fromElements()
    ds
  }

  private  def  unknownData2 ={
    val ds: DataSet[(String, String)] =  env.fromElements()
    ds
  }

  private  def  unknownData3 ={
    val ds: DataSet[(String, String, String)] =  env.fromElements()
    ds
  }

  private  def  unknownData4 ={
    val ds: DataSet[(String, String, String, String)] =  env.fromElements()
    ds
  }

  //DATA
  private lazy val v760  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v760.csv").map(stringMapper4)
  private lazy val v515  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v515.csv").map(stringMapper4)
  private lazy val v905  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v905.csv").map(stringMapper4)
  private lazy val v14  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v14.csv").map(stringMapper4)
  private lazy val v781  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v781.csv").map(stringMapper4)
  private lazy val v763  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v763.csv").map(stringMapper4)
  private lazy val m139004  = unknownData4
  private lazy val v466  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v466.csv").map(stringMapper4)
  private lazy val v199  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v199.csv").map(stringMapper4)
  private lazy val v308  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v308.csv").map(stringMapper4)
  private lazy val v708  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v708.csv").map(stringMapper4)
  private lazy val v762  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v762.csv").map(stringMapper4)
  private lazy val v174  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v174.csv").map(stringMapper4)
  private lazy val v748  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v748.csv").map(stringMapper4)
  private lazy val m249004  = unknownData4
  private lazy val m56004  = unknownData4
  private lazy val v653  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v653.csv").map(stringMapper4)
  private lazy val v929  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v929.csv").map(stringMapper4)
  private lazy val v635  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v635.csv").map(stringMapper4)
  private lazy val v51  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v51.csv").map(stringMapper4)
  private lazy val v197  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v197.csv").map(stringMapper4)
  private lazy val v22  = env.readTextFile("hdfs:///user/hduser/stype/resources/benchmark/100/data/v22.csv").map(stringMapper4)

  //Rewriting
  private lazy val p6= (((((((((((v905.map(t=> (t._2,t._2)) union v22.map(t=> (t._1,t._1)))
    union v781.map(t=> (t._1,t._1)))
    union v51.map(t=> (t._1,t._1)))
    union v653.map(t=> (t._1,t._1)))
    union m56004.map(t=> (t._2,t._2)))
    union v308.map(t=> (t._1,t._1)))
    union v466.map(t=> (t._1,t._1)))
    union v762.map(t=> (t._1,t._1)))
    union v635.map(t=> (t._2,t._2)))
    union v197.map(t=> (t._1,t._1)))
    union m139004.map(t=> (t._1,t._1)))

  private lazy val p1= (((((((((p6.join(v929).where(0,0).equalTo(0,2).count()>0 || p6.join(v14).where(0).equalTo(0).count()>0)
    || p6.join(v748).where(0).equalTo(0).count()>0)
    || p6.join(v515).where(0).equalTo(0).count()>0)
    || p6.join(v708).where(0).equalTo(1).count()>0)
    || m249004.join(p6).where(0).equalTo(0).count()>0)
    || p6.join(v760).where(0).equalTo(1).count()>0)
    || p6.join(v174).where(0).equalTo(3).count()>0)
    || p6.join(v199).where(0,0).equalTo(0,1).count()>0)
    || p6.join(v763).where(0).equalTo(0).count()>0)


  println(s"p6: ${p6.print()}")

  println(s"p1: $p1")


}