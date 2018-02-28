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

import fr.lirmm.graphik.graal.api.core.Term




/**
  * Created by
  * Salvatore Rapisarda
  * Stanislav Kikot
  * on 28/04/2017.
  */
case class Splitter(root: TreeDecomposition) {
  val separator: TreeDecomposition = root.getSeparator
  val children: List[Splitter] = {
    val directChildren = separator.getChildes.map(Splitter)
    if (separator != root) {
      val rootGeneratedChild: TreeDecomposition = root.remove(separator)
      Splitter(rootGeneratedChild) :: directChildren
    }
    else directChildren
  }

  def getSplittingVertex: Bag = separator.getRoot

  override def toString: String = {
    s"(root: $root, children: $children)"
  }

  def getAllTerms : Set[Term] = root.getAllTerms

}
