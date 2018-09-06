package ODBASE

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

import java.util.Date

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by:
  * Salvatore Rapisarda
  * Stanislav Kikot
  *
  */
trait BaseFlinkRewriting {

  val log: Logger = LoggerFactory.getLogger(this.getClass)
  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  // change pathToHadoopFolder variable in order to point to the correct folder in hadoop
  val pathToHadoopFolder = "hdfs:///user/hduser/stypes/resources/benchmark/Lines"
  implicit val typeLongInfo: TypeInformation[(Long, Long)] = TypeInformation.of(classOf[(Long, Long)])
  implicit val typeRelation2Info: TypeInformation[Relation2] = TypeInformation.of(classOf[Relation2])

  case class Relation2(x: Long, y: Long)

  def longMapper: (String) => (Long, Long) = (p: String) => {
    val line = p.split(',')
    (line.head.toLong, line.last.toLong)
  }

  def stringMapper1: (String) => (String) = (p: String) => {
    val line = p.split(',')
    line.head
  }

  def stringMapper: (String) => (String, String) = (p: String) => {
    val line = p.split(',')
    (line.head, line.last)
  }

  def stringMapper3: (String) => (String, String, String) = (p: String) => {
    val line = p.split(',')
    (line(0), line(1), line(2))
  }

  def stringMapper4: (String) => (String, String, String, String) = (p: String) => {
    val line = p.split(',')
    (line(0), line(1), line(2), line(3))
  }


  def rel2Mapper: (String) => Relation2 = (p: String) => {
    val line = p.split(',')
    Relation2(line.head.toLong, line.last.toLong)
  }

  def myJoin(firstRelation: DataSet[(String, String)], secondRelation: DataSet[(String, String)]): DataSet[(String, String)] = {
    firstRelation.join(secondRelation).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
  }

  def switchTerms(relation: DataSet[(String, String)]): DataSet[(String, String)] = relation.map(p => (p._2, p._1))

  def unknownData2: DataSet[(String, String)] = {
    val ds: DataSet[(String, String)] = env.fromElements()
    ds
  }


  def getA(fileNumber: Int): DataSet[(String, String)] =
    env.readTextFile(s"$pathToHadoopFolder/data/csv/$fileNumber.ttl-A.csv").map(stringMapper)

  def getB(fileNumber: Int): DataSet[(String, String)] =
    env.readTextFile(s"$pathToHadoopFolder/data/csv/$fileNumber.ttl-B.csv").map(stringMapper)

  def getR(fileNumber: Int): DataSet[(String, String)] =
    env.readTextFile(s"$pathToHadoopFolder/data/csv/$fileNumber.ttl-R.csv").map(stringMapper)

  def getS(fileNumber: Int): DataSet[(String, String)] =
    env.readTextFile(s"$pathToHadoopFolder/data/csv/$fileNumber.ttl-S.csv").map(stringMapper)


  def execute(fileNumber: Int, serial: String, qName: String, f: (Int) => DataSet[(String, String)]): Unit = {
    val startTime = System.nanoTime()
    distinctSink(f.apply(fileNumber), fileNumber, serial, startTime, qName)
  }


  def distinctSink(p1: DataSet[(String, String)], fileNumber: Int, serial: String, startTime: Long, qName: String): Unit = {
    val p1_distinct = p1.distinct()

    val postfix = s"ttl-$fileNumber-par-${env.getParallelism}-${new Date().getTime}"
    val resultPath = s"$pathToHadoopFolder/data/results/$qName/$serial/results-$postfix"
    p1_distinct.writeAsCsv(resultPath)

    val count: Long = p1_distinct.count
    val elapsed = (System.nanoTime() - startTime) / 1000000
    log.info(s"elapsed time for $postfix is: $elapsed")

    val qe: DataSet[String] = env.fromElements(fileNumber.toString, env.getParallelism.toString, elapsed.toString, count.toString, resultPath)
    qe.writeAsText(s"$pathToHadoopFolder/data/results/$qName/$serial/result-$postfix-txt")

  }
}
