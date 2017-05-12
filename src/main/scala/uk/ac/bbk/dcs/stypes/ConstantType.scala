package uk.ac.bbk.dcs.stypes


import fr.lirmm.graphik.graal.api.core.{AbstractTerm, Term}

/**
  * Created by :
  *   Salvatore Rapisarda
  *   Stanislav Kikot
  *
  * <p> on 12/05/2017.
  */
class ConstantType( identifier: (Int, String) ) extends AbstractTerm {

  // PUBLIC METHODS
  override def isConstant: Boolean = true

  override def getType: Term.Type = Term.Type.LITERAL

  override def getIdentifier: (Int, String)  = identifier

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

}
