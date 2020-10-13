package uk.ac.bbk.dcs.stypes.evaluate

import fr.lirmm.graphik.graal.api.core.{Atom, Term}

trait SelectJoinAtoms {
  val lhs: Option[AnyRef]
  val rhs: Option[AnyRef]
  val joined: List[Term]
  val projected: List[Term]
}

object SelectJoinAtoms {
  def empty: SelectJoinAtoms = new SelectJoinAtoms {
    override val lhs: Option[AnyRef] = None
    override val rhs: Option[AnyRef] = None
    override val joined: List[Term] = Nil
    override val projected: List[Term] = Nil
  }
}
case class SingleSelectJoinAtoms(lhs: Option[(Atom, Int)], rhs: Option[(Atom, Int)], joined: List[Term], projected: List[Term])
  extends SelectJoinAtoms

case class MultiSelectJoinAtoms(lhs: Option[SelectJoinAtoms], rhs: Option[(Atom, Int)], joined: List[Term], projected: List[Term])
  extends SelectJoinAtoms