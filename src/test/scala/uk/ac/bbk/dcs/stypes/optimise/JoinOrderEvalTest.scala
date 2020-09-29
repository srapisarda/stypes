package uk.ac.bbk.dcs.stypes.optimise

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate}
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import org.scalatest.FunSuite
import uk.ac.bbk.dcs.stypes.Clause

import scala.util.Random

class JoinOrderEvalTest extends FunSuite {

  test("testOptimiseJoinOrderEval") {
    val atoms = getAtoms
    val catalog = getCatalogStatistics(atoms)
    val clause = getClause(atoms)
    val ordered = JoinOrderEval.optimiseClauseJoinOrderEval(clause, catalog)
    println(ordered)
    println(catalog)
    val l = ordered.body.map(catalog.atomStatisticMap(_).rowCount)
    assert(l == l.sorted)
  }

  test("testOptimise") {

  }

  private def getClause(body: List[Atom]) = {
    val head: Atom = new DefaultAtom(new Predicate(s"h", 2))
    head.setTerm(0, DefaultTermFactory.instance.createVariable("X0"))
    head.setTerm(1, DefaultTermFactory.instance.createVariable("X9"))
    Clause(head, body)
  }

  private def getAtoms: List[Atom] = {

    val atoms: List[Atom] =
      for {x <- (0 until 10).toList} yield {
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
