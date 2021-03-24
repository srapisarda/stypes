package uk.ac.bbk.dcs.stypes.sql

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import net.sf.jsqlparser.expression.Alias
import net.sf.jsqlparser.expression.operators.relational.EqualsTo
import net.sf.jsqlparser.schema.{Column, Table}
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.{FromItem, Join, PlainSelect, Select, SelectExpressionItem, SelectItem, SubSelect}
import uk.ac.bbk.dcs.stypes.Clause

import scala.collection.JavaConverters._

object SqlUtils {

  /**
    * The EDBs are atoms present in the body but not in the head a clause.
    * The IDBs are atoms defined in the head of a clause.
    *
    * @return set of Predicate
    */
  def getEdbPredicates(ndl: List[Clause]): Set[Predicate] = {
    val iDbPredicates = ndl.map(p => p.head.getPredicate).distinct
    ndl.flatten(_.body.map(_.getPredicate).distinct)
      .filter(!iDbPredicates.contains(_)).toSet
  }

  def ndl2sql(ndl: List[Clause], goalPredicate: Predicate, dbCatalog: EDBCatalog): Statement = {
    var clauseToMap = ndl.toSet
    var aliasIndex = 0
    val eDbPredicates = getEdbPredicates(ndl)

    def removeClauseToMap(clause: Clause): Unit = clauseToMap -= clause

    def increaseAliasIndex(): Unit = aliasIndex += 1

    def getJoinsFromClause(clause: Clause) = {


      var atomToJoin: Set[Atom] = clause.body.tail.toSet
      val atomIndexed = clause.body.toArray
      val joins: List[Join] = atomIndexed.tail.map(atom => {
        atomToJoin -= atom
        val join = new Join()
        join.setInner(true)

        val rightItem =
          if (eDbPredicates.contains(atom.getPredicate)) {
            val tableName = atom.getPredicate.getIdentifier.toString
            val table = new Table(tableName)
            table.setAlias(new Alias(s"$tableName$aliasIndex"))
            increaseAliasIndex()
            table
          } else {
            new SubSelect()
          }
        join.setRightItem(rightItem)
        val onExpression = new EqualsTo()
        //        val term: Term = ??? //todo:
        //        onExpression.setLeftExpression(new Column(getTableFromTerm(term, clause), term.getIdentifier.toString))
        //        join.setOnExpression(onExpression)
        join
      }).toList
      joins
    }

    def getSelectFromBody(atom: Atom, aliasIndex: Int): FromItem = {
      if (eDbPredicates.contains(atom.getPredicate)) {
        val identifier = atom.getPredicate.getIdentifier.toString
        val from = new Table(identifier)
        from.setAlias(new Alias(s"$identifier$aliasIndex"))
        from
      } else {
        getSubSelect(atom)
      }
    }

    def getSubSelect(atom: Atom): SubSelect = {
      val subSelect = new SubSelect()

      val clause = clauseToMap
        .find(_.head.getPredicate == atom.getPredicate)
        .getOrElse(throw new RuntimeException("Atom is not present in select"))

      subSelect.setSelectBody(getSelectBody(clause))

      subSelect
    }

    def getSelectExpressionItem(term: Term, clauseBodyWithIndex: List[(Atom, Int)]) = {
      val bodyAtomsIndexed: (Atom, Int) = clauseBodyWithIndex
        .find(_._1.contains(term))
        .getOrElse(throw new RuntimeException("Term not in body clause!"))

      val table = new Table(s"${bodyAtomsIndexed._1.getPredicate.getIdentifier.toString}${bodyAtomsIndexed._2}")
      val catalogAtom: Atom = dbCatalog.getAtomFromPredicate(bodyAtomsIndexed._1.getPredicate)
        .getOrElse(throw new RuntimeException("Predicate not present in EDB Catalog!"))

      val sqlTerm = catalogAtom.getTerm(bodyAtomsIndexed._1.indexOf(term))
      new SelectExpressionItem(new Column(table, sqlTerm.getIdentifier.toString))
    }

    def getSelectBody(clause: Clause) = {

      removeClauseToMap(clause)

      def atom = clause.head

      def clauseBodyWithIndex: List[(Atom, Int)] = clause.body.zipWithIndex

      val columns: List[SelectItem] = atom.getTerms.asScala.map(
        getSelectExpressionItem(_, clauseBodyWithIndex)
      ).toList

      val selectBody = new PlainSelect()
      val fromItem = getSelectFromBody(clause.body.head, aliasIndex)
      increaseAliasIndex()
      selectBody.setFromItem(fromItem)
      selectBody.setJoins(getJoinsFromClause(clause).asJava)
      selectBody.setSelectItems(columns.asJava)

      // add union

      selectBody
    }

    //
    val clause = clauseToMap
      .find(_.head.getPredicate == goalPredicate)
      .getOrElse(throw new RuntimeException("Goal predicate is not present"))

    val select = new Select
    select.setSelectBody(getSelectBody(clause))
    select

  }
}
