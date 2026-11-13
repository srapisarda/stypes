package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term, Variable}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.mutable
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.sys.process._
import scala.util.control.NonFatal

class PostgresLeafIntegrationTest extends AnyFunSpec with BeforeAndAfterAll {
  private val composeFile = "src/test/postgres/compose.yaml"
  private val composeProject = s"stypes-leaf-${UUID.randomUUID().toString.replace("-", "")}"
  private val compose = Seq("docker", "compose", "-p", composeProject, "-f", composeFile)

  private def runCompose(arguments: String*): Unit = {
    val output = mutable.ListBuffer.empty[String]
    val exit = Process(compose ++ arguments).!(ProcessLogger(output += _, output += _))
    require(exit == 0, s"Docker Compose ${arguments.mkString(" ")} failed:\n${output.mkString("\n")}")
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    try runCompose("up", "-d", "--wait")
    catch {
      case NonFatal(startError) =>
        try runCompose("down", "--volumes", "--remove-orphans")
        catch { case NonFatal(cleanupError) => startError.addSuppressed(cleanupError) }
        throw startError
    }
  }

  override protected def afterAll(): Unit = {
    try runCompose("down", "--volumes", "--remove-orphans")
    finally super.afterAll()
  }

  private def fixture(name: String): String = {
    val source = Source.fromFile(s"src/test/postgres/$name.sql")
    try source.mkString finally source.close()
  }

  private def queryPostgres(sql: String): Map[String, Set[List[String]]] = {
    val output = mutable.ListBuffer.empty[String]
    val errors = mutable.ListBuffer.empty[String]
    val command = compose ++ Seq(
      "exec", "-T", "postgres", "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1",
      "-U", "stypes", "-d", "stypes")
    val input = new ByteArrayInputStream(sql.getBytes(StandardCharsets.UTF_8))
    val exit = (Process(command) #< input).!(ProcessLogger(output += _, errors += _))
    assert(exit == 0, s"PostgreSQL command failed:\n${errors.mkString("\n")}")
    output.toList.map(_.split("\\|", -1).toList).groupBy(_.head)
      .map { case (label, rows) => label -> rows.map(_.tail).toSet }
  }

  // This small test-only translator evaluates the generated Datalog independently
  // of the production SQL translator; all predicates in these fixtures are positive.
  private def rewritingSql(clauses: List[Clause], answerVariables: List[Variable]): String = {
    val definitions = clauses.groupBy(_.head.getPredicate)
    val goal = definitions.keys.collectFirst {
      case p: DatalogPredicate if p.isGoalPredicate => p
    }.getOrElse(fail("Rewriting has no goal predicate"))
    val ordered = mutable.ListBuffer.empty[Predicate]
    val visited = mutable.Set.empty[Predicate]
    val visiting = mutable.Set.empty[Predicate]

    def visit(predicate: Predicate): Unit = {
      if (!visited.contains(predicate)) {
        require(!visiting.contains(predicate), s"Recursive predicate: $predicate")
        visiting += predicate
        definitions(predicate).flatMap(_.body.map(_.getPredicate))
          .filter(definitions.contains).distinct.foreach(visit)
        visiting -= predicate
        visited += predicate
        ordered += predicate
      }
    }

    def identifier(value: Any): String = "\"" + value.toString.replace("\"", "\"\"") + "\""

    def select(clause: Clause): String = {
      val bindings = mutable.Map.empty[Term, String]
      val conditions = mutable.ListBuffer.empty[String]
      val tables = mutable.ListBuffer.empty[String]
      val equalities = mutable.ListBuffer.empty[Atom]

      clause.body.zipWithIndex.foreach { case (atom, index) =>
        if (atom.getPredicate.getIdentifier.toString == Equality.predicateName) {
          equalities += atom
        } else {
          val alias = s"t$index"
          val predicate = atom.getPredicate
          tables += s"${identifier(predicate.getIdentifier)} $alias"
          atom.getTerms.asScala.zipWithIndex.foreach { case (term, position) =>
            require(term.getType == Term.Type.VARIABLE, s"Unsupported non-variable term: $term")
            val column = if (definitions.contains(predicate)) s"X$position" else s"c$position"
            val expression = s"$alias.${identifier(column)}"
            bindings.get(term) match {
              case Some(previous) => conditions += s"$previous = $expression"
              case None => bindings.put(term, expression)
            }
          }
        }
      }
      equalities.foreach { atom =>
        conditions += s"${bindings(atom.getTerm(0))} = ${bindings(atom.getTerm(1))}"
      }
      require(tables.nonEmpty, s"Unsupported empty rule body: $clause")
      val projection = clause.head.getTerms.asScala.zipWithIndex.map { case (term, position) =>
        s"${bindings(term)} AS ${identifier(s"X$position")}"
      }.mkString(", ")
      val where = if (conditions.isEmpty) "" else conditions.mkString(" WHERE ", " AND ", "")
      s"SELECT $projection FROM ${tables.mkString(", ")}$where"
    }

    visit(goal)
    val ctes = ordered.map { predicate =>
      s"${identifier(predicate.getIdentifier)} AS (${definitions(predicate).map(select).mkString(" UNION ")})"
    }.mkString(", ")
    require(goal.getArity == answerVariables.size, "Goal arity does not match the query head")
    val columns = answerVariables.indices.map(index => identifier(s"X$index")).mkString(", ")
    s"WITH $ctes SELECT DISTINCT $columns FROM ${identifier(goal.getIdentifier)}"
  }

  private def compare(fixtureName: String, decomposition: (TreeDecomposition, List[Variable]),
                      ontology: String, referenceSql: String, expected: Set[List[String]],
                      setup: String = ""): Unit = {
    val rules = if (ontology.isEmpty) List.empty else ReWriter.getOntology(ontology)
    val rewriting = new ReWriter(rules).rewrite(decomposition, addAdditionalRules = true, splitMode = Leaf)
    val centroidRewriting = new ReWriter(rules).rewrite(decomposition, addAdditionalRules = true, splitMode = Centroid)
    assert(rewriting.nonEmpty)
    assert(centroidRewriting.nonEmpty)
    val sql = s"""
      ${fixture(fixtureName)}
      $setup
      SELECT 'reference', result.* FROM ($referenceSql) result;
      SELECT 'rewriting', result.* FROM (${rewritingSql(rewriting, decomposition._2)}) result;
      SELECT 'centroid', result.* FROM (${rewritingSql(centroidRewriting, decomposition._2)}) result;
    """
    val results = queryPostgres(sql)
    assert(results.getOrElse("reference", Set.empty) == expected)
    assert(results.getOrElse("rewriting", Set.empty) == expected,
      s"Answer variables: ${decomposition._2}; goal clauses: ${rewriting.filter(_.head.getPredicate match {
        case p: DatalogPredicate => p.isGoalPredicate
        case _ => false
      }).mkString("\n")}")
    assert(results.getOrElse("centroid", Set.empty) == expected)
  }

  describe("b-depth-leaves rewriting on PostgreSQL") {
    it("agrees with the original query over the chase, including anonymous witnesses") {
      val query = TreeDecomposition.getTreeDecomposition("src/test/resources/q09.gml", "src/test/resources/q09.cq")
      val reference = """
        WITH chase_r AS (
          SELECT c0, c1 FROM r UNION ALL
          SELECT 'null:a:' || c0, c0 FROM a UNION ALL
          SELECT c0, 'null:b:' || c0 FROM b
        ), chase_s AS (
          SELECT c0, c1 FROM s UNION ALL
          SELECT c0, 'null:a:' || c0 FROM a UNION ALL
          SELECT 'null:b:' || c0, c0 FROM b
        )
        SELECT DISTINCT r0.c0, r8.c1
        FROM chase_r r0 JOIN chase_r r1 ON r0.c1 = r1.c0
        JOIN chase_s s2 ON r1.c1 = s2.c0
        JOIN chase_r r3 ON s2.c1 = r3.c0
        JOIN chase_s s4 ON r3.c1 = s4.c0
        JOIN chase_r r5 ON s4.c1 = r5.c0
        JOIN chase_s s6 ON r5.c1 = s6.c0
        JOIN chase_r r7 ON s6.c1 = r7.c0
        JOIN chase_r r8 ON r7.c1 = r8.c0
        WHERE r0.c0 NOT LIKE 'null:%' AND r8.c1 NOT LIKE 'null:%'
      """
      compare("lines", query, "src/test/resources/lines.dlp", reference,
        Set(List("c0", "c9"), List("d0", "d9"), List("e0", "e9")))
      compare("lines", query, "src/test/resources/lines.dlp", reference, Set.empty,
        "TRUNCATE a, b, r, s;")
    }

    it("agrees with the original query for a branching decomposition") {
      val query = TreeDecomposition.getTreeDecomposition("src/test/resources/q11.gml", "src/test/resources/q11.txt")
      val reference = """
        SELECT DISTINCT m.c0 FROM m286004 m
        JOIN m252004 a ON m.c0 = a.c1
        JOIN m165004 b ON m.c0 = b.c0
        JOIN m297004 c ON m.c0 = c.c0
        JOIN m203004 d ON m.c0 = d.c0
      """
      compare("branching", query, "", reference, Set(List("good")))
    }

    it("preserves shared anonymous witnesses across branches") {
      val query = TreeDecomposition.getTreeDecomposition(
        "src/test/postgres/branching-witness.gml", "src/test/postgres/branching-witness.cq")
      val reference = """
        WITH chase_path AS (
          SELECT c0, c1 FROM path UNION ALL
          SELECT c0, 'null:y:' || c0 FROM seed
        ), chase_q AS (
          SELECT c0, c1 FROM edgeq UNION ALL
          SELECT 'null:y:' || c0, 'null:z:' || c0 FROM seed
        ), chase_r AS (
          SELECT c0, c1 FROM edger UNION ALL
          SELECT 'null:y:' || c0, 'null:z:' || c0 FROM seed
        )
        SELECT DISTINCT p.c0 FROM chase_path p
        JOIN chase_q q ON p.c1 = q.c0
        JOIN chase_r r ON p.c1 = r.c0
        WHERE p.c0 NOT LIKE 'null:%'
      """
      compare("branching-witness", query, "src/test/postgres/branching-witness.dlp", reference,
        Set(List("ordinary"), List("from_seed")))
    }
  }
}
