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
  * Created by
  * Salvatore Rapisarda
  * Stanislav Kikot,
  * Roman Kontchakov
  * on 28/04/2017.
  */
case class Splitter(root: TreeDecomposition, boundaryRoot: Option[TreeDecomposition] = None, boundaryLeaf: Option[TreeDecomposition] = None) {
  private val logger = Logger(this.getClass)
  logger.debug(s"b Splitter creating: ${root.getRoot}, boundaryRoot: ${boundaryRoot.map(_.getRoot)}, boundaryLeaf: ${boundaryLeaf.map(_.getRoot)}")

  val splittingBag: TreeDecomposition = //root.getCentroid//root.getCentroid
    if (boundaryLeaf.isEmpty ) {
      logger.debug(s"\tcalling centroid")
      root.getCentroid
    } else {
      logger.debug(s"\tcalling lemma 7.5")
      // degree is 2, so both boundary root and leaf are non-null
      val centroid = root.getCentroid
      val cPath = root.getPathTo(centroid)
      val bPath = root.getPathTo(boundaryLeaf.get)
      if ( cPath.size <= 2 && cPath.size > bPath.size) {
        logger.debug(s"\t\tcentroid cPath.size: ${cPath.size} bPath.size: ${bPath.size}")
        centroid
      }
      else {
        logger.debug(s"\t\tlast common vertex  cPath.size: ${cPath.size} bPath.size: ${bPath.size}")
        TreeDecomposition.getLastCommonVertex(cPath, bPath)
      }
    }
  // root.getSeparator
  logger.debug(s"\tSplitting bag ${splittingBag.hashCode()} : ${splittingBag.getRoot} ")

  val children: List[Splitter] = root.split(splittingBag).map(subSplitter)

//  logger.debug(s"Splitter ${splittingBag.hashCode()} Children : $children")

  logger.debug(s"e Splitter created: $this")

  def getSplittingVertex: Bag = splittingBag.getRoot

  private def subSplitter(subtree: TreeDecomposition) = {
    if (boundaryLeaf.nonEmpty && subtree.contains(boundaryLeaf.get.getRoot))
      Splitter(subtree, Some(subtree), boundaryLeaf)
    else if (boundaryRoot.nonEmpty && subtree.contains(boundaryRoot.get.getRoot))
      Splitter(subtree, Some(subtree), boundaryRoot)
    else
      Splitter(subtree, Some(subtree), None)
  }

  override def toString: String = {
    s"(hash: ${splittingBag.hashCode()} splittingBag: ${splittingBag.getRoot.atoms}, children: ${children.map( _.root.getRoot.atoms)}), " +
      s"boundaryRoot: ${if (boundaryRoot.isDefined) boundaryRoot.get.getRoot.atoms else Set()}" +
      s", boundaryLeaf: ${if (boundaryLeaf.isDefined) boundaryLeaf.get.getRoot.atoms else Set()}"
  }

  def getAllTerms: Set[Term] = root.getAllTerms

  def flattenLog(splitter: Splitter = this, parent: Option[TreeDecomposition] = None,
                 boundaryRoot: Option[TreeDecomposition] = None,
                 boundaryLeaf: Option[TreeDecomposition] = None ): List[String] = {
    List(s"splitterBag: ${splitter.splittingBag.getRoot.atoms}, children: ${splitter.children.size}, " +
      s"parent: ${if (parent.isDefined) parent.get.getRoot.atoms else ""}" +
      s", boundaryRoot: ${if (boundaryRoot.isDefined) boundaryRoot.get.getRoot.atoms else "null"}" +
      s", boundaryLeaf: ${if (boundaryLeaf.isDefined) boundaryLeaf.get.getRoot.atoms else "null"}") :::
      splitter.children.flatMap(t =>flattenLog(t, Some(splitter.splittingBag),t.boundaryRoot,t.boundaryLeaf))
  }
}
