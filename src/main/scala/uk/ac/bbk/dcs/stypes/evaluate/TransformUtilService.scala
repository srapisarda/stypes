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

  def getRhs(lhsTerms: List[Term],
             termsMapToAtom: Map[Term, List[(Int, (Atom, Int))]],
             evaluated: List[(Atom, Int)]): Option[(Atom, Int)] = {
    // it gets all possible atoms that have common terms and are not
    // element of the current left-hand-side (lhs)
    val associatedToTerms =
    lhsTerms
      .filter(termsMapToAtom.contains)
      .map(term => (term, termsMapToAtom(term)
        .filter(value => !evaluated.contains(value._2))
      ))

    //if any terms are present return
    if (associatedToTerms.isEmpty) None
    else {
      // now it select as right-hand-side (rhs) that atom with
      // the minimum index in the list
      val rhs: (Atom, Int) = associatedToTerms
        .flatMap(p => p._2)
        .map(p => p._2)
        .minBy(p => p._2)

      Some(rhs)
    }
  }

  @scala.annotation.tailrec
  def joinClauseAtom(head: Atom, bodyMapped: Map[Atom, Int],
                     termsMapToAtom: Map[Term, List[(Int, (Atom, Int))]],
                     current: SelectJoinAtoms, evaluated: List[(Atom, Int)] = Nil): SelectJoinAtoms = {

    //<editor-fold desc="internal help function">
    def getTermToProject(lhsTerms: List[Term], rhsTerms: List[Term], nextBody: Map[Atom, Int]) = {
      val nextBodyTerms: List[Term] = nextBody.keys.toList.flatMap(_.getTerms.asScala.toList)
      rhsTerms.union(lhsTerms)
        .filter(term => head.getTerms.asScala.contains(term)
          || nextBodyTerms.contains(term))
    }

    def getNext(lhsTerms: List[Term], rhs: (Atom, Int), curr: SelectJoinAtoms, isSingleReturned: Boolean): SelectJoinAtoms = {
      val nextBody = bodyMapped - rhs._1
      val rhsTerms = rhs._1.getTerms.asScala.toList
      val projected: List[Term] = getTermToProject(lhsTerms, rhsTerms, nextBody)

      if (isSingleReturned)
        SingleSelectJoinAtoms(curr.asInstanceOf[SingleSelectJoinAtoms].lhs, Some(rhs),
          lhsTerms.filter(rhsTerms.contains),
          projected)
      else
        MultiSelectJoinAtoms(Some(curr),
          Some(rhs),
          projected.filter(rhsTerms.contains), projected)
    }
    //</editor-fold>

    if (bodyMapped.isEmpty) current
    else if (current.lhs.isEmpty) {
      val bodyHead = bodyMapped.head
      val nextBody = bodyMapped - bodyHead._1
      val first = SingleSelectJoinAtoms(Some(bodyHead), None, Nil,
        getTermToProject(bodyHead._1.getTerms.asScala.toList, Nil, nextBody))
      joinClauseAtom(head, nextBody, termsMapToAtom, first, bodyHead :: evaluated)
    } else if (current.rhs.isEmpty) {
      val lhsTerms = current.lhs.asInstanceOf[Option[(Atom, Int)]].get._1.getTerms.asScala.toList
      val rhs = getRhs(lhsTerms, termsMapToAtom, evaluated)
      if (rhs.isEmpty) current
      else joinClauseAtom(head, bodyMapped - rhs.get._1, termsMapToAtom,
        getNext(lhsTerms, rhs.get, current, isSingleReturned = true),
        rhs.get :: evaluated)
    } else {
      val rhs = getRhs(current.projected, termsMapToAtom, evaluated)
      if (rhs.isEmpty) current
      else joinClauseAtom(head, bodyMapped - rhs.get._1, termsMapToAtom,
        getNext(current.projected, rhs.get, current, isSingleReturned = false),
        rhs.get :: evaluated)
    }
  }

  val clauseAsFlinkScript: Clause => String = (clause) => {
    // termsMapToAtom variable to a atom, atom-index and its position in the atom
    val atomsWithIndex = clause.body.zipWithIndex
    val termsMapToAtom = mapTermToAtoms(atomsWithIndex)
    //    println((clause, termsMapToAtom))
    val joinAtoms = joinClauseAtom(clause.head, atomsWithIndex.toMap, termsMapToAtom, SelectJoinAtoms.empty)
    println(s"${clause}  ----->   ${joinAtoms}")

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

  private def mapEdb(program: String, request: FlinkProgramRequest)

  = {
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

  private def mapEdbResource(property: EdbProperty, mapperName: String): String

  = property.fileType match {
    case _ => "env.readTextFile(\"" + property.path + "\")" + s".map($mapperName)"
  }
}
