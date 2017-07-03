package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.{Atom, Term}

import scala.collection.JavaConverters._

/**
  * Created by:
  *   Salvatore Rapisarda
  *   Stanislav Kikot
  *
  * on 27/03/2017.
  */
case class Bag( atoms:Set[Atom], variables: Set[Term]  ) {
  // val variables: List[Term] = atoms.flatMap(p=> p.getTerms.asScala)

  override def toString:String= {
    s"(atoms: $atoms, variables: $variables)"
  }
}
