package uk.ac.bbk.dcs.stypes

/*
 * #%L
 * stype
 * %%
 * Copyright (C) 2017 - 2021 Birkbeck University of London
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
