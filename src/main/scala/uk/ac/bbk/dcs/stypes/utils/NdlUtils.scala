package uk.ac.bbk.dcs.stypes.utils

import fr.lirmm.graphik.graal.api.core.Predicate
import uk.ac.bbk.dcs.stypes.{Clause, ReWriter}

import scala.annotation.tailrec

object NdlUtils {
  /**
    * The EDBs are atoms present in the body but not in the head of a clause.
    * The IDBs are atoms defined as the head of a clause.
    *
    * @return set of Predicate
    * */
  def getIdbPredicates(ndl: List[Clause]): Set[Predicate] = {
    ndl.map(p => p.head.getPredicate).toSet
  }

  def getEdbPredicates(ndl: List[Clause], optIDbPredicates: Option[Set[Predicate]] = None): Set[Predicate] = {
    val iDbPredicates = optIDbPredicates.getOrElse(getIdbPredicates(ndl))
    ndl.flatten(_.body.map(_.getPredicate).distinct)
      .filter(!iDbPredicates.contains(_)).toSet
  }

  def getGoalPredicate(ndl: List[Clause]): Predicate = {
    val iDBs = getIdbPredicates(ndl)
    val predicatesClauseBodyContains = ndl.flatten(clause => clause.body.map(atom => atom.getPredicate).toSet)
    iDBs.find(predicate => !predicatesClauseBodyContains.contains(predicate))
      .getOrElse(throw new RuntimeException("The NDL does not contains any goal predicate"))
  }

  def getDepthByIdbPredicate(ndl: List[Clause], goalPredicateOption: Option[Predicate] = None): Int = {
    val goalPredicate = goalPredicateOption.getOrElse(getGoalPredicate(ndl))
    val eDBs = getEdbPredicates(ndl)
    val ndlGroupedByPredicate: Map[Predicate, List[Predicate]] = ndl
      .groupBy(_.head.getPredicate)
      .map {
        case (predicate, clauses) =>
          predicate -> clauses.flatMap(_.body.map(_.getPredicate)).filterNot(eDBs).distinct
      }

    @tailrec
    def bfs(toVisit: List[Predicate], visited: Set[Predicate], depth: Int): Int = toVisit match {
      case List() => depth
      case predicate :: tail =>
        if (visited.contains(predicate))
          bfs(tail, visited, depth)
        else
          bfs(tail ++ ndlGroupedByPredicate(predicate), visited + predicate, depth + 1)
    }

    bfs(List(goalPredicate), Set(), 0)
  }

  def getIdbPredicatesDefCount(ndl: List[Clause]): Map[String, (Int, Int)] = {
    val iDBs = getIdbPredicates(ndl)

    def increment(map: scala.collection.mutable.Map[String, (Int, Int)], predicateId: String, value: Int, isHead: Boolean): Unit = {
      if (map.contains(predicateId))
        map(predicateId) =
          if (isHead) (map(predicateId)._1 + value, map(predicateId)._2)
          else (map(predicateId)._1, map(predicateId)._2 + value)
      else if (isHead) map(predicateId) = (value, 0)
      else map(predicateId) = (0, value)
    }

    @tailrec
    def getIdbPredicatesDefCountH(ndl: List[Clause],
                                  map: scala.collection.mutable.Map[String, (Int, Int)] = scala.collection.mutable.Map()):
    scala.collection.mutable.Map[String, (Int, Int)] = ndl match {
      case List() => map
      case clause :: tail =>

        if (iDBs.contains(clause.head.getPredicate)) {
          val piHead = clause.head.getPredicate.getIdentifier.toString
          increment(map, piHead, 1, isHead = true)
        }

        val pisBody = clause.body.map(_.getPredicate).groupBy(identity).mapValues(_.size)
        pisBody.foreach {
          case (predicate: Predicate, value: Int) =>
            if (iDBs.contains(predicate)) {
              increment(map, predicate.getIdentifier.toString, value = value, isHead = false)
            }
        }
        getIdbPredicatesDefCountH(tail, map)
    }

    getIdbPredicatesDefCountH(ndl).toMap
  }

  def main(args: Array[String]): Unit = {
    if (args.isEmpty || args.length < 2) println("Please provide the path to NDL file and option (depth|goal|edb|idb)")
    else {
      val ndl = ReWriter.getDatalogRewriting(args(0))
      args(1) match {
        case "depth" => println(getDepthByIdbPredicate(ndl, Some(getGoalPredicate(ndl))))
        case "goal" => println(getGoalPredicate(ndl))
        case "edb" => println(getEdbPredicates(ndl))
        case "idb" => println(getIdbPredicates(ndl))
        case "idb-def-count" =>
          val filename = args(0).replaceAll("^.*[/]", "")
          println(getIdbPredicatesDefCount(ndl)
            .map(p=> s"$filename,${p._1},${p._2._1},${p._2._2}")
            .mkString("\n"))
      }
    }
  }
}
