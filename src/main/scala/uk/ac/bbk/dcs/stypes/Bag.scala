package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.graal.api.core.Atom
import scala.collection.JavaConverters._

/**
  * Created by:
  *   Salvatore Rapisarda
  *   Stanislav Kikot
  *
  * on 27/03/2017.
  */
case class Bag( atoms:Set[Atom]  ) {
  val variables = atoms.flatMap( p=> p.getTerms.asScala)
}
