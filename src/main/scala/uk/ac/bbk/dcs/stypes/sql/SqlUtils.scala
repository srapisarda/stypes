package uk.ac.bbk.dcs.stypes.sql

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import net.sf.jsqlparser.expression.Alias
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

  def ndl2sql(ndl: List[Clause], goalPredicate: Predicate): Statement = {
    var clauseToMap = ndl.toSet
    var aliasIndex = 0
    val eDbPredicates = getEdbPredicates(ndl)


    def removeClauseToMap(clause: Clause): Unit = clauseToMap -= clause

    def increaseAliasIndex(): Unit = aliasIndex += 1

    def getTableFromTerm(term: Term, clause: Clause): Table = {
      val name = clause.body
        .find(_.contains(term))
        .getOrElse(throw new RuntimeException("Term not in body clause!"))
        .getPredicate.getIdentifier.toString
      new Table(name)
    }

    def getJoinsFromClause(clause: Clause) = {

      var atomToJoin = clause.body.toSet

      clause.body.map(atom => {
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
        //join.setOnExpression()
      })
    }


    def getSelectFromBody(atom: Atom, aliasIndex: Int): FromItem = {
      if (eDbPredicates.contains(atom.getPredicate)) {
        val identifier = atom.getPredicate.getIdentifier.toString
        val from = new Table(identifier)
        from.setAlias(new Alias(s"identifier$aliasIndex"))
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

    def getSelectBody(clause: Clause) = {

      removeClauseToMap(clause)

      val columns: List[SelectItem] = clause.head.getTerms.asScala.map(term =>
        new SelectExpressionItem(new Column(getTableFromTerm(term, clause), term.getIdentifier.toString))
      ).toList

      val selectBody = new PlainSelect()
      selectBody.setSelectItems(columns.asJava)
      val fromItem = getSelectFromBody(clause.body.head, aliasIndex)
      increaseAliasIndex()
      selectBody.setFromItem(fromItem)
      //selectBody.setJoins(getJoinsFromClause(clause).asJava)

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
