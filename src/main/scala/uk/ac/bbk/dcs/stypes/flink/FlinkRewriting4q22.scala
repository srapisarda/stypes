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
object FlinkRewriting4q22 {
  private val log = LoggerFactory.getLogger("FlinkRewriting4q22")
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
        FlinkRewriting4q22.execute(args(0).toInt, args(1))
      else
        FlinkRewriting4q22.execute(args(0).toInt)
  }

  def execute(fileNumber:Int, serial:String=UUID.randomUUID().toString):Unit = {

    val startTime = System.nanoTime()

    val pathToBenchmarkNDL_SQL = "hdfs:///user/hduser/stype/resources/benchmark/Lines"

    val a: DataSet[(String, String)] = env.readTextFile(s"$pathToBenchmarkNDL_SQL/data/csv/${fileNumber}.ttl-A.csv").map(stringMapper)
    val b: DataSet[(String, String)] = env.readTextFile(s"$pathToBenchmarkNDL_SQL/data/csv/${fileNumber}.ttl-B.csv").map(stringMapper)
    val r: DataSet[(String, String)] = env.readTextFile(s"$pathToBenchmarkNDL_SQL/data/csv/${fileNumber}.ttl-R.csv").map(stringMapper)
    val s: DataSet[(String, String)] = env.readTextFile(s"$pathToBenchmarkNDL_SQL/data/csv/${fileNumber}.ttl-S.csv").map(stringMapper)


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


    val postfix= s"ttl-${fileNumber}-par-${env.getParallelism}-${new Date().getTime}"

    val p1_distinct = p1.distinct()

    val resultPath =s"$pathToBenchmarkNDL_SQL/data/results/q22/$serial/results-$postfix"
    p1_distinct.writeAsCsv(resultPath)

    val count: Long = p1_distinct.count
    val elapsed = (System.nanoTime() - startTime)  / 1000000
    log.info(s"elapsed time for $postfix is: $elapsed")

    val qe: DataSet[String] = env.fromElements( fileNumber.toString, env.getParallelism.toString, elapsed.toString , count.toString, resultPath )
    qe.writeAsText(s"$pathToBenchmarkNDL_SQL/data/results/q22/$serial/result-$postfix-txt")

    val count2: Long = p1_distinct.count

    log.info(s"p1_distinct count: $count")


  }


  case class QueryEvaluation( ttl: Int, parallelism: Int, milliseconds:Long, count:Long, resultPath:String    )

}
