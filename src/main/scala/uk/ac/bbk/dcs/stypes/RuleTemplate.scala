package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.core.DefaultAtom
import scala.collection.JavaConverters._

/**
  * Created by
  *   Salvatore Rapisarda
  *   Stanislav Kikot
  *
  * on 25/07/2017.
  */
class RuleTemplate (splitter: Splitter, borderType:Type, generatingAtoms:List [Atom]) {

  val terms:List[Term] = borderType.getVar(generatingAtoms)

  def getNewPredicate : Predicate = new Predicate( (splitter, borderType) , terms.size )

  val head:Atom = new DefaultAtom(getNewPredicate, terms.asJava)

}

