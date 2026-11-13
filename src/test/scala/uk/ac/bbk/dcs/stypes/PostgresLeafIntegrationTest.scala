package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term, Variable}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import uk.ac.bbk.dcs.stypes.sql.{EDBCatalog, SqlUtils}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
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

  private def cliSql(clauses: List[Clause], catalogPath: String): String = {
    val datalogFile = Files.createTempFile("stypes-q15-", ".dlp")
    try {
      Files.write(datalogFile, clauses.mkString("\n").getBytes(StandardCharsets.UTF_8))
      val output = new ByteArrayOutputStream()
      val stream = new PrintStream(output, true, StandardCharsets.UTF_8.name())
      try Console.withOut(stream) {
        SqlUtils.main(Array(datalogFile.toString, catalogPath))
      } finally stream.close()
      val sql = output.toString(StandardCharsets.UTF_8.name()).trim
      assert(sql.nonEmpty, "SQL command-line entry point produced no output")
      sql
    } finally Files.deleteIfExists(datalogFile)
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
  private def rewritingSql(clauses: List[Clause], answerArity: Int,
                           goalOverride: Option[Predicate] = None,
                           legacyLowercaseVariables: Boolean = false): String = {
    val definitions = clauses.groupBy(_.head.getPredicate)
    val goal = goalOverride.orElse(definitions.keys.collectFirst {
      case p: DatalogPredicate if p.isGoalPredicate => p
    }).getOrElse(fail("Rewriting has no goal predicate"))
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
            // Legacy rewriting fixtures use lowercase identifiers for variables.
            require(term.getType == Term.Type.VARIABLE || legacyLowercaseVariables,
              s"Unsupported non-variable term: $term")
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
    require(goal.getArity == answerArity, "Goal arity does not match the query head")
    val columns = (0 until answerArity).map(index => identifier(s"X$index")).mkString(", ")
    s"WITH $ctes SELECT DISTINCT $columns FROM ${identifier(goal.getIdentifier)}"
  }

  private def compare(fixtureName: String, decomposition: (TreeDecomposition, List[Variable]),
                      ontology: String, referenceSql: String, expected: Set[List[String]],
                      setup: String = "", productionCatalog: Option[String] = None): Unit = {
    val rules = if (ontology.isEmpty) List.empty else ReWriter.getOntology(ontology)
    val rewriting = new ReWriter(rules).rewrite(decomposition, addAdditionalRules = true, splitMode = Leaf)
    val centroidRewriting = new ReWriter(rules).rewrite(decomposition, addAdditionalRules = true, splitMode = Centroid)
    assert(rewriting.nonEmpty)
    assert(centroidRewriting.nonEmpty)
    def productionSql(clauses: List[Clause], catalogPath: String): String = {
      val goal = clauses.collectFirst {
        case clause if clause.head.getPredicate match {
          case predicate: DatalogPredicate => predicate.isGoalPredicate
          case _ => false
        } => clause.head.getPredicate
      }.getOrElse(fail("Rewriting has no goal predicate"))
      SqlUtils.ndl2sql(clauses, goal, EDBCatalog.getEDBCatalogFromFile(catalogPath), useWith = true).toString
    }
    val productionSelect = productionCatalog.map { catalogPath =>
      val leafSql = productionSql(rewriting, catalogPath)
      val centroidSql = productionSql(centroidRewriting, catalogPath)
      val leafCliSql = cliSql(rewriting, catalogPath)
      val centroidCliSql = cliSql(centroidRewriting, catalogPath)
      s"""SELECT 'production_leaf', result.* FROM ($leafSql) result;
         |SELECT 'production_centroid', result.* FROM ($centroidSql) result;
         |SELECT 'cli_leaf', result.* FROM ($leafCliSql) result;
         |SELECT 'cli_centroid', result.* FROM ($centroidCliSql) result;""".stripMargin
    }.getOrElse("")
    val sql = s"""
      ${fixture(fixtureName)}
      $setup
      SELECT 'reference', result.* FROM ($referenceSql) result;
      SELECT 'rewriting', result.* FROM (${rewritingSql(rewriting, decomposition._2.size)}) result;
      SELECT 'centroid', result.* FROM (${rewritingSql(centroidRewriting, decomposition._2.size)}) result;
      $productionSelect
    """
    val results = queryPostgres(sql)
    assert(results.getOrElse("reference", Set.empty) == expected)
    assert(results.getOrElse("rewriting", Set.empty) == expected,
      s"Answer variables: ${decomposition._2}; goal clauses: ${rewriting.filter(_.head.getPredicate match {
        case p: DatalogPredicate => p.isGoalPredicate
        case _ => false
      }).mkString("\n")}")
    assert(results.getOrElse("centroid", Set.empty) == expected)
    if (productionCatalog.nonEmpty) {
      assert(results.getOrElse("production_leaf", Set.empty) == expected)
      assert(results.getOrElse("production_centroid", Set.empty) == expected)
      assert(results.getOrElse("cli_leaf", Set.empty) == expected)
      assert(results.getOrElse("cli_centroid", Set.empty) == expected)
    }
  }

  describe("SQL rewriting fixtures on PostgreSQL") {
    val cases = List(
      ("q07-rew_test-with.sql", "q07-rew_test.dlp", 1, false),
      ("q15-rew-n-with-alias.sql", "q15-rew-n.dlp", 2, true),
      ("q15-rew-with.sql", "q15-rew.dlp", 2, false),
      ("q22-rew_test-with.sql", "q22-rew_test.dlp", 2, false),
      ("q22-rew_test-with-alias.sql", "q22-rew_test.dlp", 2, true),
      ("q45-rew_test-with-alias.sql", "q45-rew_test.dlp", 2, true))

    cases.foreach { case (sqlFile, datalogFile, arity, hasAliases) =>
      it(s"executes $sqlFile and agrees with its Datalog rewriting") {
        val directory = "src/test/resources/rewriting/"
        val source = Source.fromFile(directory + sqlFile)
        val sql = try source.mkString.trim.stripSuffix(";") finally source.close()
        val clauses = ReWriter.getDatalogRewriting(directory + datalogFile)
        val expectedSql = rewritingSql(clauses, arity, Some(new Predicate("p1", arity)),
          legacyLowercaseVariables = true)
        val projection = if (hasAliases) "result.x, result.y" else "result.*"
        val results = queryPostgres(s"""
          ${fixture("lines")}
          INSERT INTO a VALUES ('match');
          INSERT INTO b VALUES ('match');
          INSERT INTO r VALUES ('match', 'match');
          INSERT INTO s VALUES ('match', 'match');
          INSERT INTO r SELECT 'path' || n, 'path' || (n + 1) FROM generate_series(0, 19) AS n;
          INSERT INTO s SELECT 'path' || n, 'path' || (n + 1) FROM generate_series(0, 19) AS n;
          INSERT INTO a VALUES ('path5'), ('path10');
          INSERT INTO b VALUES ('path5'), ('path10');
          ALTER TABLE a ADD COLUMN x text GENERATED ALWAYS AS (c0) STORED,
                        ADD COLUMN a1 text GENERATED ALWAYS AS (c0) STORED;
          ALTER TABLE b ADD COLUMN x text GENERATED ALWAYS AS (c0) STORED,
                        ADD COLUMN a1 text GENERATED ALWAYS AS (c0) STORED;
          ALTER TABLE r ADD COLUMN x text GENERATED ALWAYS AS (c0) STORED,
                        ADD COLUMN y text GENERATED ALWAYS AS (c1) STORED,
                        ADD COLUMN a1 text GENERATED ALWAYS AS (c0) STORED,
                        ADD COLUMN a2 text GENERATED ALWAYS AS (c1) STORED;
          ALTER TABLE s ADD COLUMN x text GENERATED ALWAYS AS (c0) STORED,
                        ADD COLUMN y text GENERATED ALWAYS AS (c1) STORED,
                        ADD COLUMN a1 text GENERATED ALWAYS AS (c0) STORED,
                        ADD COLUMN a2 text GENERATED ALWAYS AS (c1) STORED;
          SELECT 'reference', result.* FROM ($expectedSql) result;
          SELECT 'snapshot', $projection FROM ($sql) result;
          TRUNCATE a, b, r, s;
          SELECT 'empty_reference', result.* FROM ($expectedSql) result;
          SELECT 'empty_snapshot', $projection FROM ($sql) result;
        """)
        val reference = results.getOrElse("reference", Set.empty)
        assert(reference.contains(List.fill(arity)("match")), s"$sqlFile did not produce the known matching row")
        if (arity == 2) assert(reference.exists(row => row.head != row(1)), s"$sqlFile has no distinct endpoint answers")
        assert(results.getOrElse("snapshot", Set.empty) == reference, s"Incorrect answers from $sqlFile")
        assert(results.getOrElse("empty_reference", Set.empty).isEmpty)
        assert(results.getOrElse("empty_snapshot", Set.empty).isEmpty)
      }
    }
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

    it("agrees with q15 over the chase for explicit and existential paths") {
      val query = TreeDecomposition.getTreeDecomposition("src/test/resources/q15.gml", "src/test/resources/q15.cq")
      val setup = """
        INSERT INTO r VALUES
          ('c10', 'c11'), ('c11', 'c12'), ('c14', 'c15'),
          ('d10', 'd11'), ('d11', 'd12'), ('d14', 'd15'),
          ('e10', 'e11'), ('e11', 'e12'), ('e14', 'e15');
        INSERT INTO s VALUES
          ('c9', 'c10'), ('c12', 'c13'), ('c13', 'c14'),
          ('d9', 'd10'), ('d12', 'd13'), ('d13', 'd14'),
          ('e9', 'e10'), ('e12', 'e13'), ('e13', 'e14');
      """
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
        SELECT DISTINCT r0.c0, r14.c1
        FROM chase_r r0 JOIN chase_r r1 ON r0.c1 = r1.c0
        JOIN chase_s s2 ON r1.c1 = s2.c0
        JOIN chase_r r3 ON s2.c1 = r3.c0
        JOIN chase_s s4 ON r3.c1 = s4.c0
        JOIN chase_r r5 ON s4.c1 = r5.c0
        JOIN chase_s s6 ON r5.c1 = s6.c0
        JOIN chase_r r7 ON s6.c1 = r7.c0
        JOIN chase_r r8 ON r7.c1 = r8.c0
        JOIN chase_s s9 ON r8.c1 = s9.c0
        JOIN chase_r r10 ON s9.c1 = r10.c0
        JOIN chase_r r11 ON r10.c1 = r11.c0
        JOIN chase_s s12 ON r11.c1 = s12.c0
        JOIN chase_s s13 ON s12.c1 = s13.c0
        JOIN chase_r r14 ON s13.c1 = r14.c0
        WHERE r0.c0 NOT LIKE 'null:%' AND r14.c1 NOT LIKE 'null:%'
      """
      compare("lines", query, "src/test/resources/lines.dlp", reference,
        Set(List("c0", "c15"), List("d0", "d15"), List("e0", "e15")), setup,
        Some("src/test/postgres/lines-catalog.dlp"))
      compare("lines", query, "src/test/resources/lines.dlp", reference, Set.empty,
        "TRUNCATE a, b, r, s;", Some("src/test/postgres/lines-catalog.dlp"))
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
