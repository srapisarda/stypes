package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import net.sf.jsqlparser.expression.Alias
import net.sf.jsqlparser.schema.{Column, Table}
import net.sf.jsqlparser.statement.Statement
import net.sf.jsqlparser.statement.select.{FromItem, FromItemVisitor, Join, Pivot, PlainSelect, Select, SelectExpressionItem, SelectItem, SubJoin, SubSelect}

import scala.collection.JavaConverters._

object SqlUtils {

  def ndl2sql(ndl: List[Clause], eDbs: Set[Atom], goalPredicate: Predicate): Statement = {

    var clauseToMap = ndl.toSet
    var aliasIndex = 0

    def removeClauseToMap(clause: Clause) = clauseToMap -= clause

    def increaseAliasIndex = aliasIndex = aliasIndex + 1

    def getTableFromTerm(term: Term, clause: Clause) = {
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
          if (eDbs.contains(atom)) {
            val tableName = atom.getPredicate.getIdentifier.toString
            val table = new Table(tableName)
            table.setAlias(new Alias(s"$tableName$aliasIndex"))
            increaseAliasIndex
            table
          } else {
            new SubSelect()
          }
        join.setRightItem(rightItem)
        //join.setOnExpression()
      })
    }


    def getSelectFromBody(atom: Atom, aliasIndex: Int): FromItem = {
      if (eDbs.contains(atom)) {
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
        .find(_.head.getPredicate==atom.getPredicate)
        .getOrElse( throw new RuntimeException("Atom is not present in select"))

      subSelect.setSelectBody( getSelectBody( clause))

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
      increaseAliasIndex
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
    select.setSelectBody( getSelectBody(clause))
    select

  }


}
