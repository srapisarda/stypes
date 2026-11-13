package uk.ac.bbk.dcs.stypes

/*
 * #%L
 * STypeS
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

import com.typesafe.scalalogging.Logger
import fr.lirmm.graphik.graal.api.core.Term




/**
  * Splitting mode for the Splitter.
  * - Centroid: uses centroid decomposition (default, for b-depth-tw).
  * - Leaf: pathifies the decomposition and repeatedly picks an endpoint (for b-depth-leaves).
  */
sealed trait SplitMode
case object Centroid extends SplitMode
case object Leaf extends SplitMode

/**
  * Created by
  * Salvatore Rapisarda
  * Stanislav Kikot
  * on 28/04/2017.
  */
case class Splitter(
    root: TreeDecomposition,
    mode: SplitMode = Centroid,
    private val pathPrepared: Boolean = false
) {
  private val splittingRoot =
    if (mode == Leaf && !pathPrepared) root.asPathDecomposition else root
  private val logger = Logger(this.getClass)
  logger.debug(s"Splitter creating with root: $splittingRoot, mode: $mode")

  val splittingBag: TreeDecomposition = mode match {
    case Centroid => splittingRoot.getCentroid
    case Leaf => splittingRoot.getLeaf
  }
  logger.debug(s"Splitting bag ${splittingBag.hashCode()} : ${splittingBag.getRoot} ")

  val children: List[Splitter] = {
    val directChildren = splittingBag.getChildren.map(c => new Splitter(c, mode, pathPrepared = true))
    if (splittingBag != splittingRoot) {
      val rootGeneratedChild: TreeDecomposition = splittingRoot.remove(splittingBag)
      new Splitter(rootGeneratedChild, mode, pathPrepared = true) :: directChildren
    }
    else directChildren
  }

  logger.debug(s"Splitter ${splittingBag.hashCode()} Children : $children")

  logger.debug(s"Splitter created: $this")

  def getSplittingVertex: Bag = splittingBag.getRoot

  override def toString: String = {
    s"(hash: ${splittingBag.hashCode()} splittingBag: $splittingBag, children: $children)"
  }

  def getAllTerms : Set[Term] = splittingRoot.getAllTerms

  def flattenLog(splitter: Splitter = this, parent:Option[TreeDecomposition] = None  ): List[String] = {
      List(s"splitterBag: ${splitter.splittingBag.getRoot.atoms}, children: ${splitter.children.size}, parent: ${if (parent.isDefined) parent.get.getRoot.atoms else "" }" ) ::: splitter.children.flatMap(flattenLog(_, Some(splitter.splittingBag)))
  }
}
