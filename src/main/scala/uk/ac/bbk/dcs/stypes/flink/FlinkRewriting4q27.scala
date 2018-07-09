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

import java.util.{Date, UUID}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.slf4j.LoggerFactory

/**
  * Created by salvo on 10/05/2018.
  *
  * uk.ac.bbk.dcs.stypes.flink.FlinkRewriting4q22
  */
object FlinkRewriting4q27 {
  private val log = LoggerFactory.getLogger("FlinkRewriting4q27")
  private val env = ExecutionEnvironment.getExecutionEnvironment

  private implicit val typeLongInfo: TypeInformation[(Long, Long)] = TypeInformation.of(classOf[(Long, Long)])

  private implicit val typeRelation2Info: TypeInformation[Relation2] = TypeInformation.of(classOf[Relation2])

  case class Relation2( x:Long, y:Long )

  private  def  longMapper: (String) => (Long, Long) = (p: String) => {
    val line = p.split(',')
    (line.head.toLong, line.last.toLong)
  }

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


  private  def  rel2Mapper: (String) => Relation2 = (p: String) => {
    val line = p.split(',')
    Relation2(line.head.toLong, line.last.toLong)
  }

  private def myJoin( firstRelation: DataSet[(String, String)], secondRelation: DataSet[(String, String)] ) ={
    firstRelation.join(secondRelation).where(1).equalTo(0).map(p=> (p._1._1, p._2._2 ))
  }

  private def switchTerms( relation: DataSet[(String, String)] ) = relation.map(p=> (p._2, p._1))

  private  def  unknownData2 ={
    val ds: DataSet[(String, String)] =  env.fromElements()
    ds
  }


  def main(args: Array[String]): Unit = {
      if ( args.length > 1 )
        FlinkRewriting4q27.execute(args(0).toInt, args(1))
      else
        FlinkRewriting4q27.execute(args(0).toInt)
  }

  def execute(fileNumber:Int, serial:String=UUID.randomUUID().toString):Unit = {

    val startTime = System.nanoTime()

    val pathToBenchmarkNDL_SQL = "hdfs:///user/hduser/stype/resources/benchmark/Lines"

    val a: DataSet[(String, String)] = env.readTextFile(s"$pathToBenchmarkNDL_SQL/data/csv/${fileNumber}.ttl-A.csv").map(stringMapper)
    val b: DataSet[(String, String)] = env.readTextFile(s"$pathToBenchmarkNDL_SQL/data/csv/${fileNumber}.ttl-B.csv").map(stringMapper)
    val r: DataSet[(String, String)] = env.readTextFile(s"$pathToBenchmarkNDL_SQL/data/csv/${fileNumber}.ttl-R.csv").map(stringMapper)
    val s: DataSet[(String, String)] = env.readTextFile(s"$pathToBenchmarkNDL_SQL/data/csv/${fileNumber}.ttl-S.csv").map(stringMapper)


    val  r_join = myJoin(r,r)

    val r_double_join = myJoin(r_join,r)
    // p32(x7,x9) :- r(x7,x8), s(x8,x9).
    // p32(x9,x9) :- b(x9).
    val p32= myJoin(r,s ).union(b)

    // p27(x12,x7) :- r(x7,x8), a(x8), r(x8,x11), r(x11,x12).
    // p27(x12,x7) :- p32(x7,x9), r(x9,x10), r(x10,x11), r(x11,x12).
    val p27_switched =  (myJoin(myJoin( r,a), r_join)  )
                    .union( ( myJoin(p32, r_double_join ) ))

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

    val postfix= s"ttl-${fileNumber}-par-${env.getParallelism}-${new Date().getTime}"

    val p1_distinct = p1.distinct()

    val resultPath =s"$pathToBenchmarkNDL_SQL/data/results/q27/$serial/results-$postfix"
    p1_distinct.writeAsCsv(resultPath)

    val count: Long = p1_distinct.count
    val elapsed = (System.nanoTime() - startTime)  / 1000000
    log.info(s"elapsed time for $postfix is: $elapsed")

    val qe: DataSet[String] = env.fromElements( fileNumber.toString, env.getParallelism.toString, elapsed.toString ,
      count.toString, resultPath )
    qe.writeAsText(s"$pathToBenchmarkNDL_SQL/data/results/q27/$serial/result-$postfix-txt")

    val count2: Long = p1_distinct.count

    log.info(s"p1_distinct count: $count")


  }


  case class QueryEvaluation( ttl: Int, parallelism: Int, milliseconds:Long, count:Long, resultPath:String    )

}
