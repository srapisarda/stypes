package uk.ac.bbk.dcs.stypes

import java.io.File

import fr.lirmm.graphik.graal.api.core.{Atom, AtomSet, Rule, RuleSet}
import fr.lirmm.graphik.graal.core.ruleset.LinkedListRuleSet
import fr.lirmm.graphik.graal.io.dlp.{DlgpParser, DlgpWriter}
import fr.lirmm.graphik.graal.store.rdbms.DefaultRdbmsStore
import fr.lirmm.graphik.graal.store.rdbms.driver.HSQLDBDriver
import org.scalatest.FunSpec

/**
  * Created by rapissal on 09/05/2017.
  */
class ReWriterTest extends FunSpec{

  // 0 - Create a Dlgp writer and a structure to store rules.
  val writer = new DlgpWriter
  val ontology = new LinkedListRuleSet

  // 1 - Create a relational database store with HSQLDB (An InMemory Java
  // database system),
  val store = new DefaultRdbmsStore(new HSQLDBDriver("test", null))

  // 2 - Parse Animals.dlp (A Dlgp file with rules and facts)
  val dlgpParser = new DlgpParser(new File("src/main/resources/ont-1.dlp" ))
  while (dlgpParser.hasNext) {
    val o = dlgpParser.next
    if (o.isInstanceOf[Atom])
        store.add(o.asInstanceOf[Atom])
    if (o.isInstanceOf[Rule])
        ontology.add(o.asInstanceOf[Rule])
  }





  describe("ReWriter test cases") {

    it("should contains at least 2 atoms") {
       val atoms =  ReWriter.generateAtoms( ontology)
        assert(atoms.size == 2)
       println (atoms)

    }

  }
}
