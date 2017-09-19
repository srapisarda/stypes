package uk.ac.bbk.dcs.stypes


import fr.lirmm.graphik.graal.api.core.{AbstractTerm, Term}


/**
  *
  * Created by :
  *   Salvatore Rapisarda
  *   Stanislav Kikot
  *
  * <p> on 12/05/2017.
  * @param identifier is a tuple (a, b) where:
  *   a - is an integer value index to the generating atom
  *   b - is an identifier of the point in the chase or canonical model
  */
class ConstantType(var identifier: (Int, String) ) extends AbstractTerm {

  def this( term: Term  ) = {
    this(term.asInstanceOf[ConstantType].getIdentifier )
  }

  // PUBLIC METHODS
  override def isConstant: Boolean = true

  override def getType: Term.Type = Term.Type.LITERAL

  override def getIdentifier: (Int, String)  = identifier

  override def getLabel: String = getIdentifier._2

  override def equals (o: Any): Boolean = {
      if ( super.equals(o) ) true
      else o match {
        case  obj: ConstantType =>
            this.identifier._1 == obj.getIdentifier._1 && this.identifier._2.equals(obj.getIdentifier._2)
        case _ => false
      }
  }
}



object ConstantType {
  val EPSILON:ConstantType = new ConstantType((-1,"epsilon"))
  val CS0:ConstantType = new ConstantType(0, "EE0")
  val CS1:ConstantType = new ConstantType(1, "EE0")
}
