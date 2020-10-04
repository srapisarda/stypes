package uk.ac.bbk.dcs.stypes.evaluate

import java.io.FileNotFoundException

import fr.lirmm.graphik.graal.api.core.Atom
import uk.ac.bbk.dcs.stypes.Clause

import scala.collection.mutable
import scala.io.Source

object TransformUtilService {
  val jobTitlePattern: String = "**JOB-TITLE**"
  val namePattern: String = "**NAME**"
  val mapperFunctionsPattern: String = "//**MAPPER-FUNC"
  val edbMapPattern: String = "//**EDB-MAP**"

  def generateFlinkProgramAsString(request: FlinkProgramRequest): String = {
    // get template
    val fileTemplate = Source.fromFile(request.properties.templatePath)
    if (fileTemplate == null)
      throw new FileNotFoundException(s"template file ${request.properties.templatePath} not found!")

    // apply properties
    var program = applyProperties(fileTemplate.mkString, request.properties)
    // map EDBs
    program = mapEdb(program, request)
    // NDL to flink
    program = ndlToFlink(program, request.datalog)

    program
  }

  def ndlToFlink(program: String, datalog: List[Clause]): String = {
    // TODO
    program
  }

  private def applyProperties(program: String, properties: FlinkProgramProperties): String = {
    program
      .replace(jobTitlePattern, properties.jobTitle)
      .replace(namePattern, properties.name)
  }

  private def mapEdb(program: String, request: FlinkProgramRequest) = {
    // EDBs mapping
    val edbMap: Map[String, (String, String)] = request.edbMap.map {
      case (atom, property) => {
        val mapperTemplateFilePath = s"${request.properties.templateMappersBaseDir}/" +
          s"${property.fileType}-mappers/" +
          s"mapper-${atom.getPredicate.getArity}.txt"
        val mapperTemplate = Source.fromFile(mapperTemplateFilePath)
        if (mapperTemplate == null)
          throw new FileNotFoundException(s"mapper template file ${mapperTemplateFilePath} not found!")
        val mapperTemplateLines = mapperTemplate.getLines().toList
        val mapperName = mapperTemplateLines.head.toString.replace("//", "").trim

        (s"\tval ${atom.getPredicate.getIdentifier.toString} = " +
          s"${mapEdbResource(property, mapperName)}",
          (mapperTemplateFilePath, mapperTemplateLines.mkString("\n")))

      }
    }
    val mapperFunctions: List[String] = edbMap.values.toMap.values.toList
    program
      .replace(edbMapPattern, (edbMapPattern :: edbMap.keySet.toList).mkString("\n"))
      .replace(mapperFunctionsPattern, (mapperFunctionsPattern :: mapperFunctions).mkString("\n"))
  }

  private def mapEdbResource(property: EdbProperty, mapperName: String): String = property.fileType match {
    case _ => "env.readTextFile(\"" + property.path + "\")" + s".map($mapperName)"
  }
}
