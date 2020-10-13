package uk.ac.bbk.dcs.stypes.evaluate

import java.io.FileNotFoundException

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import uk.ac.bbk.dcs.stypes.Clause

import scala.io.Source
import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.collection.{immutable, mutable}

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

  def mapTermToAtoms(atomsWithIndex: Seq[(Atom, Int)]): Map[Term, List[(Int, (Atom, Int))]] = {
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


  @scala.annotation.tailrec
  def joinClauseAtom(head: Atom, bodyMapped: Map[Atom, Int],
                     termsMapToAtom: Map[Term, List[(Int, (Atom, Int))]],
                     current: SelectJoinAtoms): SelectJoinAtoms = {
    if (bodyMapped.isEmpty) current
    else if (current.lhs.isEmpty) {
      val bodyHead = bodyMapped.head
      val first = SingleSelectJoinAtoms(Some(bodyHead), None, Nil, Nil)
      joinClauseAtom(head, bodyMapped - bodyHead._1, termsMapToAtom, first)
    } else {
      current match {
        case curr: SingleSelectJoinAtoms =>
          if ( curr.rhs.isEmpty ) {
            // it gets all terms of the current SelectJoinAtoms
            val lhsTerms = curr.lhs.get._1.getTerms.asScala.toList
            // it gets all possible atoms that have common terms and are not
            // element of the current left-hand-side (lhs)
            val associatedToTerms =
            lhsTerms
              .filter(termsMapToAtom.contains)
              .map(term => (term, termsMapToAtom(term)
                .filter(value => value._2 != curr.lhs.get)
              ))
            // now it select as right-hand-side (rhs) that atom with
            // the minimum index in the list
            val rhs: (Atom, Int) = associatedToTerms
              .flatMap(p => p._2)
              .map(p => p._2)
              .minBy(p => p._2)

            val bodyHead = bodyMapped.head
            val nextBody = bodyMapped - bodyHead._1 - rhs._1
            val nextBodyTerms: List[Term] = nextBody.keys.toList.flatMap(_.getTerms.asScala.toList)
            val rhsTerms = rhs._1.getTerms.asScala
            val joined: List[Term] = lhsTerms.filter(rhsTerms.contains)
            val projected: List[Term] =
              rhsTerms.union(lhsTerms)
                .filter(term => head.getTerms.asScala.contains(term)
                  || nextBodyTerms.contains(term)).toList

            joinClauseAtom(head, nextBody, termsMapToAtom,
              SingleSelectJoinAtoms(curr.lhs, Some(rhs), joined, projected))
          } else {
            MultiSelectJoinAtoms(Some(curr), None, Nil, Nil);
          }
        // set the  to
        case curr: MultiSelectJoinAtoms =>
          // TODO
          val bodyHead = bodyMapped.head
          joinClauseAtom(head, bodyMapped - bodyHead._1, termsMapToAtom, current)
      }
    }
  }

  val clauseAsFlinkScript: Clause => String = (clause) => {
    // termsMapToAtom variable to a atom, atom-index and its position in the atom
    val atomsWithIndex = clause.body.zipWithIndex
    val termsMapToAtom = mapTermToAtoms(atomsWithIndex)
    //    println((clause, termsMapToAtom))
    val joinAtoms = joinClauseAtom(clause.head, atomsWithIndex.toMap, termsMapToAtom, SelectJoinAtoms.empty)
    println(joinAtoms)

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
