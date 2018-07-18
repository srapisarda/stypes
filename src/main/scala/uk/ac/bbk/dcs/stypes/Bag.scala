package uk.ac.bbk.dcs.stypes

/*
 * #%L
 * stypes
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

import fr.lirmm.graphik.graal.api.core.{Atom, Term}


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
