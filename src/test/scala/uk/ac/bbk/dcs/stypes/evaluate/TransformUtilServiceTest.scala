package uk.ac.bbk.dcs.stypes.evaluate

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate}
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.scalatest.FunSuite
import uk.ac.bbk.dcs.stypes.{Clause, ReWriter}

class TransformUtilServiceTest extends FunSuite {
  val datalog = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q30-rew.dlp")
  printDatalog(datalog)

  val request: FlinkProgramRequest = FlinkProgramRequest(
    datalog,
    getEdbMap(List("s", "r", "a")),
    FlinkProgramProperties("test", "job test",
      "src/main/resources/templates/flink-template.txt",
      "src/main/resources/templates", getSinkPath))


  private def getSinkPath = {
    "hdfs:////user/hduser/data/report2020/result/" +
      s"${java.time.LocalDateTime.now().toString.replace(":", "-")}.csv"
  }

  private def getEdbMap(identifiers: List[String]) = {
    identifiers.map(identifier => {
      val dataPath = "hdfs:////user/hduser/data/report2020"
      val edb: Atom = new DefaultAtom(new Predicate(identifier, 2))
      edb.setTerm(0, DefaultTermFactory.instance.createVariable("X1"))
      edb.setTerm(1, DefaultTermFactory.instance.createVariable("X2"))
      edb -> EdbProperty(s"$dataPath/$identifier.csv", "csv")
    }).toMap
  }

  test("testGenerateFlinkProgramAsString") {

    val programAsString = TransformUtilService.generateFlinkProgramAsString(request)
    println(programAsString)

    val expectedMap = List("val s = env.readTextFile(\"hdfs:////user/hduser/data/report2020/s.csv\").map(stringMapper2)",
      "val r = env.readTextFile(\"hdfs:////user/hduser/data/report2020/r.csv\").map(stringMapper2)",
      "val a = env.readTextFile(\"hdfs:////user/hduser/data/report2020/a.csv\").map(stringMapper2)")
    expectedMap.foreach(a => assert(programAsString.contains(a)))
  }

  private def printDatalog(datalog: List[Clause]): Unit =
    println(s"${datalog.mkString(".\n")}.".replaceAll("""\[\d+\]""", ""))
}
