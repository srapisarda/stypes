package uk.ac.bbk.dcs.stypes.flink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
  * Created by salvo on 16/07/2018.
  *
  **/
trait BaseFlinkTest {

  val pathToBenchmarkNDL_SQL = "src/test/resources/benchmark/Lines"

  val conf = new Configuration()
  conf.setInteger("taskmanager.numberOfTaskSlots", 4)

  val env = ExecutionEnvironment.createLocalEnvironment(conf)

  env.setParallelism(1)

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

  def myJoin(firstRelation: DataSet[(String, String)], secondRelation: DataSet[(String, String)]) = {
    if ( firstRelation.first(1).count() <= 0 || secondRelation.first(1).count()<=0 )
      emptyData2
    else
      firstRelation.join(secondRelation).where(1).equalTo(0).map(p => (p._1._1, p._2._2))
  }

  def switchTerms(relation: DataSet[(String, String)]) = relation.map(p => (p._2, p._1))

  def emptyData2 = {
    val ds: DataSet[(String, String)] = env.fromElements()
    ds
  }

  def getA(fileNumber: Int): DataSet[(String, String)] =
    env.readTextFile(s"$pathToBenchmarkNDL_SQL/data/csv/${fileNumber}.ttl-A.csv").map(stringMapper)

  def getB(fileNumber: Int): DataSet[(String, String)] =
    env.readTextFile(s"$pathToBenchmarkNDL_SQL/data/csv/${fileNumber}.ttl-B.csv").map(stringMapper)

  def getR(fileNumber: Int): DataSet[(String, String)] =
    env.readTextFile(s"$pathToBenchmarkNDL_SQL/data/csv/${fileNumber}.ttl-R.csv").map(stringMapper)

  def getS(fileNumber: Int): DataSet[(String, String)] =
    env.readTextFile(s"$pathToBenchmarkNDL_SQL/data/csv/${fileNumber}.ttl-S.csv").map(stringMapper)

}
