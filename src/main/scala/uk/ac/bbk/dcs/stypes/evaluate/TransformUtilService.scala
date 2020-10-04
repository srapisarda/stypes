package uk.ac.bbk.dcs.stypes.evaluate

import java.io.FileNotFoundException

import scala.io.Source

object TransformUtilService {
  val jobTitlePattern: String = "**JOB-TITLE**"
  val namePattern: String = "**NAME**"
  val mapperFunctions: String = "//**MAPPER-FUNC"
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

    program
  }

  private def applyProperties(program: String, properties: FlinkProgramProperties): String = {
    program
      .replace(jobTitlePattern, properties.jobTitle)
      .replace(namePattern, properties.name)
  }

  private def mapEdb(program: String, request: FlinkProgramRequest) = {
    // EDBs mapping
    val edbMap = request.edbMap.map {
      case (atom, property) =>
        s"\tval ${atom.getPredicate.getIdentifier.toString} = " +
          s"${mapEdbResource(property)}"
    }
    program.replace(edbMapPattern, (edbMapPattern :: edbMap.toList).mkString("\n"))
  }

  private def mapEdbResource(property: EdbProperty): String = property.fileType match {
    case _ => "env.readTextFile(\"" + property.path + "\").map(stringMapper)"
  }


}
