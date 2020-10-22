package uk.ac.bbk.dcs.stypes.evaluate

import java.io.FileNotFoundException

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import uk.ac.bbk.dcs.stypes.Clause

import scala.annotation.tailrec
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

  def mapTermToAtoms(atomsWithIndex: Seq[(Atom, Int)]): Map[Term, List[(Int, (Atom, Int))]] = {
    var map: mutable.Map[Term, List[(Int, (Atom, Int))]] = mutable.Map()
    atomsWithIndex.foreach {
      case (atom, idx) =>
        atom.getTerms.asScala.toList.foreach(t => {
          if (map.contains(t)) {
            map += t -> ((atom.indexOf(t), (atom, idx)) :: map(t))
          } else {
            map += t -> ((atom.indexOf(t), (atom, idx)) :: Nil)
          }
        })
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

  @tailrec
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

    def getNext(curr: SelectJoinAtoms, rhs: (Atom, Int), isSingleReturned: Boolean): SelectJoinAtoms = {
      val nextBody = bodyMapped - rhs._1
      val rhsTerms = rhs._1.getTerms.asScala.toList

      def getSingleTermWithIndex(terms: List[Term], lhs: Atom, rhs: Atom) = {
        (terms.map(t => (t, rhs.indexOf(t))) :::
          terms.map(t => (t, lhs.indexOf(t))))
          .filter(t => t._2 > -1).distinct
      }

      def getMultiTermWithIndex(terms: List[Term]) = {
        curr.projected.filter(t => terms.contains(t._1)) :::
          terms.map(t => (t, rhs._1.indexOf(t)))
            .filter(t => t._2 > -1).distinct
      }

      if (isSingleReturned) {
        val lhs = curr.asInstanceOf[SingleSelectJoinAtoms].lhs
        val lhsTerms = lhs.get._1.getTerms.asScala.toList
        val projected: List[Term] = getTermToProject(lhsTerms, rhsTerms, nextBody)
        val projectedWithIndex = getSingleTermWithIndex(projected, lhs.get._1, rhs._1)
        val joined = lhsTerms.filter(rhsTerms.contains)
        val joinWithIndex = getSingleTermWithIndex(joined, lhs.get._1, rhs._1)

        SingleSelectJoinAtoms(lhs, Some(rhs), joinWithIndex, projectedWithIndex)
      } else {
        val lhsTerms = curr.projected.map(_._1)
        val projected = getTermToProject(lhsTerms, rhsTerms, nextBody)
        val projectedWithIndex = getMultiTermWithIndex(projected)
        val joined = lhsTerms.filter(rhsTerms.contains)
        val joinWithIndex = getMultiTermWithIndex(joined)

        MultiSelectJoinAtoms(Some(curr), Some(rhs), joinWithIndex, projectedWithIndex)
      }
    }
    //</editor-fold>

    if (bodyMapped.isEmpty) current
    else if (current.lhs.isEmpty) {
      val bodyHead = bodyMapped.head
      val nextBody = bodyMapped - bodyHead._1
      val termsToProject = getTermToProject(bodyHead._1.getTerms.asScala.toList, Nil, nextBody)
      val termsToProjectWithPosition: List[(Term, Int)] = termsToProject.map(t => (t, bodyHead._1.indexOf(t)))
      val first = SingleSelectJoinAtoms(Some(bodyHead), None, Nil, termsToProjectWithPosition)
      joinClauseAtom(head, nextBody, termsMapToAtom, first, bodyHead :: evaluated)
    } else if (current.rhs.isEmpty) {
      val lhs = current.lhs.asInstanceOf[Option[(Atom, Int)]].get
      val lhsTerms = lhs._1.getTerms.asScala.toList
      val rhs = getRhs(lhsTerms, termsMapToAtom, evaluated)
      if (rhs.isEmpty) current
      else {
        joinClauseAtom(head, bodyMapped - rhs.get._1, termsMapToAtom,
          getNext(current, rhs.get, isSingleReturned = true),
          rhs.get :: evaluated)
      }
    } else {
      val rhs = getRhs(current.projected.map(_._1), termsMapToAtom, evaluated)
      if (rhs.isEmpty) current
      else joinClauseAtom(head, bodyMapped - rhs.get._1, termsMapToAtom,
        getNext(current, rhs.get, isSingleReturned = false),
        rhs.get :: evaluated)
    }
  }

  val clauseAsFlinkScript: Clause => String = clause => {
    // termsMapToAtom variable to a atom, atom-index and its position in the atom
    val atomsWithIndex = clause.body.zipWithIndex
    val termsMapToAtom = mapTermToAtoms(atomsWithIndex)
    //    println((clause, termsMapToAtom))
    val joinAtoms = joinClauseAtom(clause.head, atomsWithIndex.toMap, termsMapToAtom, SelectJoinAtoms.empty)
    println(s"$clause ----->   $joinAtoms")
    val script = generateClauseFlinkScript(clause.head, joinAtoms, "")
    println(script)
    println()
    ""
  }

  def generateClauseFlinkScript(head: Atom, joinAtoms: SelectJoinAtoms, script: String, first: Boolean = true): String = {
    val getProjectedTermsForHeadAsScript: String = {
      val termToMap = joinAtoms.projected
        .map { case (term, _) => (term, head.indexOf(term)) }
        .filter { case (_, index) => index > -1 }
        .sortBy { case (_, index) => index }
        .map { case (_, index) => s"p._$index" }
        .mkString(",")

      s"map(p => ($termToMap))"
    }

    def getProjectedAsScript: String = {
      val rhs = joinAtoms.rhs.asInstanceOf[Option[(Atom, Int)]].get._1
      val termsIdsMarked = joinAtoms.projected
        .groupBy(_._1) // group by terms
        .map { g => (g._1, g._2.head._2) } // remove terms duplicate
        .map { case (term, idx) => (term, idx, rhs.indexOf(term)) } // mark left terms with -1
      val leftIndex = termsIdsMarked.filter(t => t._2 != t._3).map(t => s"p._1._${t._2 + 1}").mkString(",") // list of lefts index
      val rightIndex = termsIdsMarked.filter(t => t._2 == t._3).map(t => s"p._1._${t._2 + 1}").mkString(",") // list of lefts index

      s"map(p => ($leftIndex, $rightIndex))"
    }

    def getJoinedAsScript: String = {
      val rhs = joinAtoms.rhs.asInstanceOf[Option[(Atom, Int)]].get._1
      val termsGroupByIndex = joinAtoms.joined.groupBy(_._1)
        .map {
          case (term, termIndexed) =>
            (term,
              termIndexed.map {
                case (term, idx) =>
                  (term, idx, rhs.indexOf(term))
              })
        }
      val lrZippedIds: List[(Int, Int)] = termsGroupByIndex
        .map(g => {
          val rhsIndex = g._2.filter(t => t._2 == t._3).map(_._2)
          val lhsIndex = g._2.filter(t => t._2 != t._3).map(_._2)
          if (lhsIndex == Nil ) (rhsIndex.head, rhsIndex.head) // it means the term is in the same position in both atoms
          else (lhsIndex.head, rhsIndex.head)
        }).toList

      val lrIds = lrZippedIds.unzip

      s"where(${lrIds._1.mkString(",")}).equalTo(${lrIds._2.mkString(",")})"
    }

    if (joinAtoms.lhs.isEmpty) {
      // just return the complete script
      if (script.isEmpty) script
      else s"${head.getPredicate.getIdentifier} = $script"
    } else if (joinAtoms.rhs.isEmpty) {
      // case when the the body contains just an atom
      // e.g. R(x,y) = S(y,x)
      val selection = getProjectedTermsForHeadAsScript
      val lhs: Atom = joinAtoms.asInstanceOf[SingleSelectJoinAtoms].lhs.get._1
      generateClauseFlinkScript(head, SelectJoinAtoms.empty,
        script = s"${lhs.getPredicate.getIdentifier}.map($selection)", first = false)
    } else {
      val joinScript = getJoinedAsScript
      // case when the the body contains more than an atom
      // e.g. R(x,w) = S(x,y), T(y,z), R(z,w)
      joinAtoms match {
        case ja: SingleSelectJoinAtoms =>
          // todo
          // check if is the first node visited therefore a
          // clause with two atoms on the body.
          // E.g.  R(x,w) = S(x,y), T(y,z)
          val projectedScript = if (first) getProjectedTermsForHeadAsScript else getProjectedAsScript
          val lhsPredicate = ja.lhs.get._1.getPredicate.getIdentifier
          val rhsPredicate = ja.rhs.get._1.getPredicate.getIdentifier
          val concatScript = if(script.isEmpty) script else s".$script"
          generateClauseFlinkScript(head, SelectJoinAtoms.empty,
            script = s"$lhsPredicate.join($rhsPredicate).$joinScript.$projectedScript\n$concatScript", first = false)

        case ja: MultiSelectJoinAtoms =>
          //todo
          val rhsPredicate = ja.rhs.get._1.getPredicate.getIdentifier
          val projectedScript = getProjectedAsScript
          val concatScript = if(script.isEmpty) script else s".$script"
          generateClauseFlinkScript(head, ja.lhs.get,
            script = s"join($rhsPredicate).$joinScript.$projectedScript\n$concatScript", first = false)
      }
    }
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
