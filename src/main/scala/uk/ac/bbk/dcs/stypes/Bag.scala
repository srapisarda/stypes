package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.Atom
import scala.collection.JavaConverters._

/**
  * Created by
  *
  *     rapissal on 24/04/2017.
  */
case class Bag( atoms:Set[Atom]  ) {
  val variables = atoms.flatMap( p=> p.getTerms.asScala)
}
