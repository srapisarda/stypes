package uk.ac.bbk.dcs.stypes.sql

import com.typesafe.scalalogging.Logger
import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import net.sf.jsqlparser.expression.Alias
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.schema.{Column, Table}
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.{FromItem, Join, PlainSelect, Select, SelectBody, SelectExpressionItem, SelectItem, SetOperation, SetOperationList, SubSelect, UnionOp, WithItem}
import uk.ac.bbk.dcs.stypes.{Clause, ReWriter}
import uk.ac.bbk.dcs.stypes.utils.NdlUtils

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object SqlUtils {
  val logger = Logger(this.getClass)
  /**
    * use
    * @param args
    */
  def main(args: Array[String]): Unit = {
    if (args.isEmpty || args.length < 2) println("Please provide di NDL and EDB catalog")
    else {
        val ndl = ReWriter.getDatalogRewriting(args(0))
        val catalog = EDBCatalog.getEDBCatalogFromFile(args(1))
        val statement = ndl2sql(ndl, new Predicate("p1", 2), catalog, useWith = true, List("x", "y"))
        println(statement.toString)
    }
  }

  @tailrec
  def orderBodyClauseByTerm(body: List[Atom], acc: List[Atom] = Nil): List[Atom] = body match {
    case Nil => acc.reverse
    case atom :: tail =>
      val atomsMatched: List[Atom] = tail.filter(_.getTerms.asScala.toSet.intersect(atom.getTerms().asScala.toSet).nonEmpty)
      val atomsNonMatched: List[Atom] = tail.filter(_.getTerms.asScala.toSet.intersect(atom.getTerms().asScala.toSet).isEmpty)
      orderBodyClauseByTerm(atomsMatched ++ atomsNonMatched, atom :: acc)
  }

  def orderNdlByTerm(ndl: List[Clause]): List[Clause] = {
    ndl.map(c => Clause(c.head, orderBodyClauseByTerm(minInFront(c.body))))
  }

  def getIdbTopologicalSorting(statPredicate: Predicate,
                               ndl: List[Clause],
                               optionIDBs: Option[Set[Predicate]] = None): List[Predicate] = {

    val iDBs = optionIDBs.getOrElse(NdlUtils.getIdbPredicates(ndl))
      val mapPredicateToIdbPredicateBody = ndl.groupBy(_.head)
      .map(group => group._1.getPredicate ->
        group._2.flatten(clause => clause.body.filter(atom => iDBs.contains(atom.getPredicate)).map(_.getPredicate)).distinct)

    logger.debug( mapPredicateToIdbPredicateBody.mkString("\n").replace("[2]", ""))

    @tailrec
    def getIdbTopologicalSortingH(toVisit: List[Predicate],
                                        visited: Set[Predicate],
                                        acc: List[Predicate]): List[Predicate] =
      toVisit match {
        case List() =>
          logger.debug(s"acc = ${acc.mkString(", ").replace("[2]","")}, toVisit = ${toVisit.mkString(",").replace("[2]","")}, visited = ${visited.mkString(",").replace("[2]","")}")
          acc
        case  predicate :: tail =>
          logger.debug(s"acc = ${acc.mkString(", ").replace("[2]","")}, toVisit = ${toVisit.mkString(",").replace("[2]","")}, visited = ${visited.mkString(",").replace("[2]","")}")
          if (!visited.contains(predicate)) {
            val dependenciesVisited = mapPredicateToIdbPredicateBody(predicate).filter(visited(_))
            val accumulator =
              if (dependenciesVisited.isEmpty) predicate :: acc
              else placePredicateAfterDependencies(acc, predicate, dependenciesVisited.toSet)
            getIdbTopologicalSortingH(
              mapPredicateToIdbPredicateBody(predicate).filterNot(visited(_)) ::: tail,
              visited + predicate,
              accumulator
            )
          } else {
//            logger.debug(s"acc = ${acc.mkString(", ").replace("[2]","")}, toVisit = ${toVisit.mkString(",").replace("[2]","")}, visited = ${visited.mkString(",").replace("[2]","")}")
            getIdbTopologicalSortingH(tail, visited, acc)
          }
      }

    @tailrec
    def placePredicateAfterDependencies(topologicalSortedList: List[Predicate],
                                        predicate: Predicate,
                                        dependencies: Set[Predicate],
                                        acc: List[Predicate] = Nil): List[Predicate] =
      topologicalSortedList match {
        case Nil =>
          val ret = acc.reverse
          logger.debug(s"ppad: predicate: ${predicate}, dependencies: ${dependencies.mkString(",")}, acc: ${ret.mkString(",")}")
          ret
        case x :: xs =>
          logger.debug(s"ppad: predicate: ${predicate}, dependencies: ${dependencies.mkString(",")}, acc: ${acc.mkString(",")}")
          if (dependencies.isEmpty) {
            logger.debug("dependencies.isEmpty")
            placePredicateAfterDependencies(Nil, predicate, dependencies, xs.reverse ::: x :: predicate :: acc)
          } else if (dependencies.contains(x)) {
            logger.debug(s"dependencies.contains($x)")
            placePredicateAfterDependencies(xs, predicate, dependencies - x, x :: acc)
          } else {
            logger.debug(s"$x visited")
            placePredicateAfterDependencies(xs, predicate, dependencies, x :: acc)
          }
      }

    getIdbTopologicalSortingH(mapPredicateToIdbPredicateBody(statPredicate), Set(statPredicate), List(statPredicate))
  }

  def minInFront(body: List[Atom]): List[Atom] = {
    if (body.isEmpty) body
    else {
      val term = body.flatten(_.getTerms().asScala).min
      val front = body.filter(_.contains(term)).sortBy(_.getTerms.size())
      val back = body.filter(!_.contains(term))
      front ::: back
    }
  }

  def ndl2sql(ndl: List[Clause], goalPredicate: Predicate,
              dbCatalog: EDBCatalog,
              useWith: Boolean = false,
              selectAlias: List[String] = Nil): Statement = {

    val ndlOrdered = orderNdlByTerm(ndl)
    var clauseToMap = ndlOrdered.toSet
    var predicateMapToSelects: Map[Predicate, Select] = Map()
    var aliasIndex = 0
    val iDbPredicates = NdlUtils.getIdbPredicates(ndlOrdered)
    val eDbPredicates = NdlUtils.getEdbPredicates(ndlOrdered, Some(iDbPredicates))

    def removeClauseToMap(clause: Clause): Unit = clauseToMap -= clause

    def increaseAliasIndex(): Unit = aliasIndex += 1

    def getMapOfCommonTermsToBodyAtomsIndexed(bodyAtomsIndexed: List[(Atom, Int)]): Map[Term, List[(Atom, Int)]] = {
      bodyAtomsIndexed.flatMap {
        case (currentAtom, _) =>
          currentAtom.getTerms.asScala.map(
            term => {
              val atomsIndexedAssociated = bodyAtomsIndexed
                .filter { case (atom, _) => atom != currentAtom && atom.getTerms.asScala.contains(term) }
              (term, atomsIndexedAssociated)

            })

      }.filterNot { case (_, listAtoms) => listAtoms == Nil }
        .groupBy { case (term, _) => term }
        .map { case (term, tupleTermListAtoms) => (term, tupleTermListAtoms.flatten(_._2).distinct.sortBy(_._2)) }
    }

    def getSelectFromBody(atom: Atom, aliasIndex: Int): FromItem = {
      if (eDbPredicates.contains(atom.getPredicate) || useWith) {
        val identifier = atom.getPredicate.getIdentifier.toString
        val from = new Table(identifier)
        from.setAlias(new Alias(s"${identifier}_$aliasIndex"))
        from
      } else {
        getSubSelect(atom, aliasIndex)
      }
    }

    def getSubSelect(atom: Atom, aliasIndex: Int): SubSelect = {

      val selectBody = if (predicateMapToSelects.contains(atom.getPredicate)) {
        predicateMapToSelects(atom.getPredicate).getSelectBody()
      } else {
        val clause = {

          clauseToMap
            .find(_.head.getPredicate == atom.getPredicate)
            .getOrElse(throw new RuntimeException(s"Atom $atom is not present in select"))
        }
        getSelect(clause.head.getPredicate).getSelectBody()
      }

      val subSelect = new SubSelect()
      subSelect.setSelectBody(selectBody)
      subSelect.setAlias(new Alias(atom.getPredicate.getIdentifier.toString + "_" + aliasIndex))
      subSelect
    }

    def getSelectExpressionItem(termIndexed: (Term, Int), clauseBodyWithIndex: List[(Atom, Int)]) = {
      val bodyAtomsIndexed: (Atom, Int) = clauseBodyWithIndex
        .find(_._1.contains(termIndexed._1))
        .getOrElse(throw new RuntimeException(s"Term $termIndexed not in body clause!"))

      val table = new Table(s"${bodyAtomsIndexed._1.getPredicate.getIdentifier.toString}_${bodyAtomsIndexed._2}")
      val atom = bodyAtomsIndexed._1

      val columnName = if (iDbPredicates.contains(bodyAtomsIndexed._1.getPredicate)) {
        val termIndex = bodyAtomsIndexed._1.indexOf(termIndexed._1)
        s"X$termIndex"
      } else {
        val sqlTerm = getTermFromCatalog(atom, termIndexed._1)
        sqlTerm.getIdentifier.toString
      }

      val selectExpressionItem = new SelectExpressionItem(new Column(table, columnName))
      selectExpressionItem.setAlias(new Alias(s"X${termIndexed._2}"))
      selectExpressionItem
    }

    def getTermFromCatalog(atom: Atom, term: Term): Term = {
      if (iDbPredicates.contains(atom.getPredicate)) {
        term
      } else {
        val catalogAtom: Atom = dbCatalog.getAtomFromPredicate(atom.getPredicate)
          .getOrElse(throw new RuntimeException(s"Predicate ${atom.getPredicate} not present in EDB Catalog!"))

        catalogAtom.getTerm(atom.indexOf(term))
      }
    }

    def getSelectBody(clause: Clause): PlainSelect = {
      removeClauseToMap(clause)
      val clauseBodyWithIndex: List[(Atom, Int)] = clause.body.zipWithIndex
      val mapOfCommonTermsToBodyAtomsIndexed = getMapOfCommonTermsToBodyAtomsIndexed(clauseBodyWithIndex)

      @tailrec
      def getSelectBodyH(head: Atom, clauseBodyIndexed: List[(Atom, Int)],
                         mapOfCommonTermsToBodyAtomsIndexed: Map[Term, List[(Atom, Int)]],
                         joins: List[Join], mappedClauseBodyIndex: List[(Term, List[(Atom, Int)])],
                         atomIndexedInSelect: Set[(Atom, Int)],
                         lastInSelect: (Atom, Int),
                         selectBody: PlainSelect): PlainSelect =
        clauseBodyIndexed match {
          case Nil =>
            selectBody.setJoins(joins.reverse.asJava)
            selectBody
          case (currentAtom, aliasIndex) :: tail =>
            if (selectBody.getFromItem == null) {
              val fromItem = getSelectFromBody(currentAtom, aliasIndex)
              selectBody.setFromItem(fromItem)
              val columns: List[SelectItem] = head.getTerms.asScala.zipWithIndex.map(
                getSelectExpressionItem(_, clauseBodyWithIndex)
              ).toList
              selectBody.setSelectItems(columns.asJava)
              getSelectBodyH(head, tail, mapOfCommonTermsToBodyAtomsIndexed, joins, mappedClauseBodyIndex,
                Set((currentAtom, aliasIndex)), (currentAtom, aliasIndex), selectBody)
            } else {
              val mapFiltered: List[(Term, List[(Atom, Int)])] = mapOfCommonTermsToBodyAtomsIndexed
                .filterNot(mappedClauseBodyIndex.contains)
                .filter(_._2.contains((currentAtom, aliasIndex)))
                .toList

              val lastItemAndCurrentJoins =
                getCurrentJoin(mapFiltered.filter(_._2.contains(lastInSelect)) ++ mapFiltered.filterNot(_._2.contains(lastInSelect)),
                  atomIndexedInSelect, (lastInSelect, List()))

              getSelectBodyH(head, tail, mapOfCommonTermsToBodyAtomsIndexed, lastItemAndCurrentJoins._2 ::: joins,
                mapFiltered ::: mappedClauseBodyIndex, atomIndexedInSelect ++ mapFiltered.flatten(_._2),
                lastItemAndCurrentJoins._1, selectBody)
            }
        }

      @tailrec
      def getCurrentJoin(mapFiltered: Seq[(Term, List[(Atom, Int)])],
                         atomIndexedInSelect: Set[(Atom, Int)],
                         acc: ((Atom, Int), List[Join])): ((Atom, Int), List[Join]) = mapFiltered match {
        case Nil =>
          acc
        case (term, atomsIndexed) :: xs =>
          val join = new Join()
          join.setInner(true)
          val rightItemOption = atomsIndexed.filter(!atomIndexedInSelect.contains(_))
          if (rightItemOption.nonEmpty) {
            val currentAtom = rightItemOption.head._1
            val aliasIndex = rightItemOption.head._2
            val rightItem = getRightJoinItem(currentAtom, aliasIndex)
            val onExpression = new EqualsTo()
            join.setRightItem(rightItem)

            val leftAtom = atomsIndexed.find(_ != (currentAtom, aliasIndex)).get
            val leftTable = new Table(leftAtom._1.getPredicate.getIdentifier.toString)
            leftTable.setAlias(new Alias(leftAtom._1.getPredicate.getIdentifier.toString + "_" + leftAtom._2))

            val rightTable = new Table(currentAtom.getPredicate.getIdentifier.toString)
            rightTable.setAlias(new Alias(currentAtom.getPredicate.getIdentifier.toString + "_" + aliasIndex))

            onExpression.setRightExpression(new Column(rightTable, getJoinExpressionColumnName(currentAtom, term)))
            onExpression.setLeftExpression(new Column(leftTable, getJoinExpressionColumnName(leftAtom._1, term)))

            join.setOnExpression(onExpression)

            val tail = if (rightItemOption.length > 1) mapFiltered else xs
            getCurrentJoin(tail, atomIndexedInSelect + rightItemOption.head, (rightItemOption.head, join :: acc._2))
          } else {
            getCurrentJoin(xs, atomIndexedInSelect, acc)
          }
      }

      getSelectBodyH(clause.head,
        clauseBodyWithIndex,
        mapOfCommonTermsToBodyAtomsIndexed,
        List(), List(), Set(),
        null,
        new PlainSelect())

    }

    def getJoinExpressionColumnName(atom: Atom, term: Term) = {
      if (iDbPredicates.contains(atom.getPredicate)) {
        s"X${atom.indexOf(term)}"
      } else {
        getTermFromCatalog(atom, term).getIdentifier.toString
      }
    }

    def getRightJoinItem(atom: Atom, aliasIndex: Int) = {
      if (eDbPredicates.contains(atom.getPredicate) || useWith) {
        val tableName = atom.getPredicate.getIdentifier.toString
        val table = new Table(tableName)
        table.setAlias(new Alias(s"${tableName}_$aliasIndex"))
        increaseAliasIndex()
        table
      } else {
        val subSelect = new SubSelect
        subSelect.setSelectBody(getSelect(atom.getPredicate, addSelectAlias = true).getSelectBody)
        subSelect.setAlias(new Alias(atom.getPredicate.getIdentifier.toString + "_" + aliasIndex))
        subSelect
      }
    }

    def getSelect(predicate: Predicate, addSelectAlias: Boolean = false): Select = {
      if (predicateMapToSelects.contains(predicate)) {
        predicateMapToSelects(predicate)
      }
      else {
        //
        val selects: List[SelectBody] = ndlOrdered
          .filter(_.head.getPredicate == predicate)
          .map(clause => getSelectBody(clause))

        if (selects.isEmpty)
          throw new RuntimeException(s"head predicate $predicate is not present")

        val select = new Select
        if (selects.size == 1) {
          select.setSelectBody(selects.head)
        } else {
          val ops: List[SetOperation] = (0 until selects.size - 1).toList.map(_ => new UnionOp())
          val sol = new SetOperationList()
          sol.withSelects(selects.asJava)
          sol.withOperations(ops.asJava)
          // sol.addBrackets(false)
          (selects.indices map (_ != 0)).foreach(sol.addBrackets(_))
          select.setSelectBody(sol)
        }

        predicateMapToSelects += predicate -> select

        select
      }
    }

    def getSelectWith(startPredicate: Predicate, selectAlias: List[String]): Select = {
      val idbDependencySpanningTreeList = getIdbTopologicalSorting(startPredicate, ndlOrdered)

      val withItems: List[WithItem] = idbDependencySpanningTreeList.map(predicate => {
        val withItem = new WithItem()
        val selectBody = getSelect(predicate).getSelectBody
        withItem.setSelectBody(selectBody)
        withItem.setName(predicate.getIdentifier.toString)
        withItem
      })

      val fromItem = new Table(startPredicate.getIdentifier.toString)
      val selectBody = new PlainSelect()
      selectBody.setFromItem(fromItem)

      val startClause = ndlOrdered.find(_.head.getPredicate == startPredicate)
        .getOrElse(throw new RuntimeException("Start predicate not found!"))

      val selectAliasVector = selectAlias.toVector
      val selectAliasVectorSize = selectAlias.size
      val selectItems: List[SelectItem] = startClause.head.getTerms.asScala.zipWithIndex.map(termIndexed => {
        val selectExpressionItem = new SelectExpressionItem(new Column(fromItem, s"X${termIndexed._2}"))
        if (selectAliasVector.nonEmpty && termIndexed._2 <= selectAliasVectorSize - 1) {
          selectExpressionItem.setAlias(new Alias(selectAliasVector(termIndexed._2)))
        }
        selectExpressionItem
      }).toList

      selectBody.setSelectItems(selectItems.asJava)

      val select = new Select()
      select.setSelectBody(selectBody)
      select.setWithItemsList(withItems.asJava)
      select
    }

    if (useWith)
      getSelectWith(goalPredicate, selectAlias)
    else
      getSelect(goalPredicate)

  }
}
