package uk.ac.bbk.dcs.stypes.optimise

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate}
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.scalatest.FunSuite
import uk.ac.bbk.dcs.stypes.{Clause, ReWriter, TestUtil}

import scala.util.Random

class JoinOrderEvalTest extends FunSuite {

  test("testOptimiseJoinOrderEval") {
    val atoms = getAtoms(10)
    val catalog = getCatalogStatistics(atoms)
    val clause = getClause(atoms, 10)

    val ordered = JoinOrderEval.optimiseClauseJoinOrderEval(clause, catalog)

    println(ordered)
    println(catalog)

    val l = ordered.body.map(catalog.atomStatisticMap(_).rowCount)
    assert(l == l.sorted)
  }

  test("testOptimise") {
    val numClauses = 5
    val numAtoms = 10
    val atoms = getAtoms(10)
    val catalogStatistics = getCatalogStatistics(atoms)

    println("catalogStatistics")
    println(catalogStatistics.atomStatisticMap.mkString("\n"))

    def makeClauses(atoms: List[Atom], acc: List[Clause] = Nil, num: Int = 0): List[Clause] = atoms match {
      case Nil => acc
      case _ :: xs =>
        if (numClauses == num) acc
        else {
          val clause = getClause(atoms, numAtoms - num)
          makeClauses(xs, clause :: acc, num + 1)
        }
    }

    val datalog = makeClauses(atoms)
    println("Datalog")
    println(datalog.mkString("\n"))

    val datalogOptimised = JoinOrderEval.optimise(datalog, catalogStatistics)

    println("Datalog Optimised")
    println(datalogOptimised.mkString("\n"))

    def op(res: Boolean, current: Clause): Boolean = {
      val l = current.body.map(catalogStatistics.atomStatisticMap(_).rowCount)
      l == l.sorted
    }

    assert(datalogOptimised.foldLeft(false)(op))
  }

  test("optimise q30")  {
    val datalog = ReWriter.getDatalogRewriting(s"src/test/resources/rewriting/q30-rew.dlp")
    TestUtil.printDatalog(datalog)


//    CatalogStatistics(
//      atoms.map(atom => atom -> EdbStatistic(Math.abs(Random.nextInt(200)))).toMap
//    )
//    val datalogOptimised = JoinOrderEval.optimise(datalog, catalogStatistics)
  }

  private def getClause(body: List[Atom], atomsInBody: Int) = {
    val head: Atom = new DefaultAtom(new Predicate(s"h", 2))
    head.setTerm(0, DefaultTermFactory.instance.createVariable("X0"))
    head.setTerm(1, DefaultTermFactory.instance.createVariable(s"X$atomsInBody"))
    Clause(head, body)
  }

  private def getAtoms(num: Int): List[Atom] = {

    val atoms: List[Atom] =
      for {x <- (0 until num).toList} yield {
        val atom1: Atom = new DefaultAtom(new Predicate(s"r$x", 2))
        atom1.setTerm(0, DefaultTermFactory.instance.createVariable(s"X$x"))
        atom1.setTerm(1, DefaultTermFactory.instance.createVariable(s"X${x + 1}"))

        atom1
      }

    atoms
  }

  private def getCatalogStatistics(atoms: List[Atom]): CatalogStatistics = {
    CatalogStatistics(
      atoms.map(atom => atom -> EdbStatistic(Math.abs(Random.nextInt(200)))).toMap
    )
  }

}
