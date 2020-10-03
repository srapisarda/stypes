package uk.ac.bbk.dcs.stypes.evaluate

import java.io.FileNotFoundException

import scala.io.Source

object TransformUtilService {
  val jobTitlePattern: String = "**JOB-TITLE**"
  val namePattern: String = "**NAME**"
  val mapperFunctions: String = "//**MAPPER-FUNC"


  def generateFlinkProgram(flinkProgramRequest: FlinkProgramRequest): String = {

    // get template
    val fileTemplate = Source.fromFile(flinkProgramRequest.properties.templatePath)
    if (fileTemplate == null)
      throw new FileNotFoundException(s"template file ${flinkProgramRequest.properties.templatePath} not found!")

    // apply properties
    var program = fileTemplate
      .mkString
      .replace(jobTitlePattern, flinkProgramRequest.properties.jobTitle)
      .replace(namePattern, flinkProgramRequest.properties.name)
    // EDBs mapping


    program
  }

  def getFlinkTemplate = ???

}
