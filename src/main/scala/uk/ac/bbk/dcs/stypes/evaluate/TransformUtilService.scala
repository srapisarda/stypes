package uk.ac.bbk.dcs.stypes.evaluate

import java.io.FileNotFoundException

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import uk.ac.bbk.dcs.stypes.Clause

import scala.io.Source
import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.collection.mutable

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

  def mapTermToAtoms(atomsWithIndex: Seq[(Atom, Int)]) = {
    var map: mutable.Map[Term, List[(Int, (Atom, Int))]] = mutable.Map()
    atomsWithIndex.foreach {
      case (atom, idx) => {
        atom.getTerms.asScala.toList.foreach(t => {
          if (map.contains(t)) {
            map += t -> ((atom.indexOf(t), (atom, idx)) :: map(t))
          } else {
            map += t -> ((atom.indexOf(t), (atom, idx)) :: Nil)
          }
        })
      }
    }
    map.toMap
  }

  def joinClauseAtom(head: Atom, bodyMapped: Map[Atom, Int],
                     termsMapToAtom: List[(Int, (Atom, Int))],
                     current: SelectJoinAtoms): SelectJoinAtoms = {
    if (bodyMapped.isEmpty) current
    else if (current.lhs.isEmpty) {
      val bodyHead = bodyMapped.head
      val first = SingleSelectJoinAtoms(Some(bodyHead), None, Nil, Nil)
      joinClauseAtom(head, bodyMapped - bodyHead._1, termsMapToAtom, first)
    } else {
      // todo
//      current match  {
//        case curr : SingleSelectJoinAtoms =>
//          termsMapToAtom.find( term =>
//            curr.lhs.get._2 != term._2._2
//              &&  bodyMapped.contains(term._2._1).
//
//
//
//
//        case curr : MultiSelectJoinAtoms =>
//      }
      joinClauseAtom(head, bodyMapped, termsMapToAtom, current)
    }
  }

  val clauseAsFlinkScript: Clause => String = (clause) => {
    // termsMapToAtom variable to a atom, atom-index and its position in the atom
    val atomsWithIndex = clause.body.zipWithIndex
    val termsMapToAtom = mapTermToAtoms(atomsWithIndex)
    println((clause, termsMapToAtom))


    ""
  }

  def clausesAsFlinkScript(head: Predicate, clauses: List[Clause]): String = {

    clauses.map(clauseAsFlinkScript)
    ""
  }

  def ndlToFlink(program: String, datalog: List[Clause]): String = {
    // create group by head predicate
    val groupByHeadPredicates: Map[Predicate, List[Clause]] = datalog.groupBy(_.head.getPredicate)
    // for each group create a UCQ as Flink script
    groupByHeadPredicates.map {
      case (head, clauses) => clausesAsFlinkScript(head, clauses)
    }
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
      case (atom, property) =>
        val mapperTemplateFilePath = s"${request.properties.templateMappersBaseDir}/" +
          s"${property.fileType}-mappers/" +
          s"mapper-${atom.getPredicate.getArity}.txt"
        val mapperTemplate = Source.fromFile(mapperTemplateFilePath)
        if (mapperTemplate == null)
          throw new FileNotFoundException(s"mapper template file $mapperTemplateFilePath not found!")
        val mapperTemplateLines = mapperTemplate.getLines().toList
        val mapperName = mapperTemplateLines.head.toString.replace("//", "").trim

        (s"\tval ${atom.getPredicate.getIdentifier.toString} = " +
          s"${mapEdbResource(property, mapperName)}",
          (mapperTemplateFilePath, mapperTemplateLines.mkString("\n")))
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
