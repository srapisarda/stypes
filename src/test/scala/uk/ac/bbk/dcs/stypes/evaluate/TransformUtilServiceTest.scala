package uk.ac.bbk.dcs.stypes.evaluate

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate}
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.scalatest.FunSuite

class TransformUtilServiceTest extends FunSuite {

  val request: FlinkProgramRequest = FlinkProgramRequest(
    List(),
    getEdbMap(List("s", "r", "a")),
    FlinkProgramProperties("test", "job test",
      "src/main/resources/templates/flink-template.txt",
      "src/main/resources/templates"))

  private def getEdbMap(identifiers: List[String]) = {
    identifiers.map(identifier => {
      val dataPath = "hdfs:////user/hduser/data/report2020"
      val edb: Atom = new DefaultAtom(new Predicate(identifier, 2))
      edb.setTerm(0, DefaultTermFactory.instance.createVariable("X1"))
      edb.setTerm(1, DefaultTermFactory.instance.createVariable("X2"))
      edb -> EdbProperty(s"$dataPath/r.csv", "csv")
    }).toMap
  }

  test("testGenerateFlinkProgramAsString") {
    def programAsString =  TransformUtilService.generateFlinkProgramAsString(request)
    println(programAsString)

    def expectedMap=List("val s = env.readTextFile(\"hdfs:////user/hduser/data/report2020/r.csv\").map(stringMapper2)",
    "val r = env.readTextFile(\"hdfs:////user/hduser/data/report2020/r.csv\").map(stringMapper2)",
    "val a = env.readTextFile(\"hdfs:////user/hduser/data/report2020/r.csv\").map(stringMapper2)")
    expectedMap.foreach(a => assert(programAsString.contains(a)))
  }


}
