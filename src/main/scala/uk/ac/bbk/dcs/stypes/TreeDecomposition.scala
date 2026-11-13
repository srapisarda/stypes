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

import com.tinkerpop.blueprints.Direction.{IN, OUT}
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.blueprints.util.io.gml.GMLReader
import com.tinkerpop.blueprints.{Edge, Graph, Vertex}
import com.typesafe.scalalogging.Logger
import fr.lirmm.graphik.graal.api.core._
import fr.lirmm.graphik.graal.api.factory.TermFactory
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import fr.lirmm.graphik.graal.io.dlp.DlgpParser

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.io.Source
import scala.util.matching.Regex

/**
  * Created by :
  *   Salvatore Rapisarda
  *   Stanislav Kikot
  * <p>
  * on 30/03/2017.
  */
class TreeDecomposition {
  private val logger = Logger(this.getClass)
  private var mapCqAtoms: Map[Predicate, Atom] = null
  private var root: Bag = null
  private var children: List[TreeDecomposition] = null
  private var cqAtoms: Set[Atom] = null

  def this(cqAtoms: Set[Atom], graph: Graph, v: Vertex, mode: Boolean = false, edgesVisited: Set[Edge]= Set()) = {
    this()

    // checks preconditions
    if (graph.getVertices.asScala.isEmpty)
      throw new RuntimeException("Vertex cannot be null!!") // Todo: Add proper exception
    this.cqAtoms = cqAtoms
    this.mapCqAtoms = cqAtoms.map(atom => atom.getPredicate -> atom).toMap
    val vertex: Vertex = if (v != null) {
      v
    } else {
      graph.getVertices.asScala.toList.minBy(v => Integer.parseInt(v.getId.toString))
    }

    root =  if (mode) getBagFromVertexTD(vertex) else getBagFromVertexHTD(vertex)

    val edgesOut = vertex.getEdges(OUT).asScala filterNot edgesVisited.contains
    val edgesIn = vertex.getEdges(IN).asScala filterNot edgesVisited.contains
    logger.debug(s"tree-decomposition-${this.hashCode()} creating, vertex: $vertex, edges out: $edgesOut, edges in: $edgesIn ")

    val childrenOut = edgesOut.map(edge =>
      new TreeDecomposition(cqAtoms,
        getSubGraph(graph, vertex, edge),
        edge.getVertex(IN), mode, edgesVisited ++ edgesOut ++ edgesIn))
      .toList

    val childrenIn = edgesIn.map(edge =>
      new TreeDecomposition(cqAtoms,
        getSubGraph(graph, vertex, edge),
        edge.getVertex(OUT), mode, edgesVisited ++ edgesOut ++ edgesIn))
      .toList

    this.children = childrenOut ::: childrenIn

    logger.debug(s"tree-decomposition-${this.hashCode()} created, root: $root, children-size: ${children.size}")

  }

  private[TreeDecomposition] def this(cqAtoms: Map[Predicate, Atom], root: Bag, children: List[TreeDecomposition]) = {
    this()
    this.mapCqAtoms = cqAtoms
    this.root = root
    this.children = children
  }


  def getSubGraph(graph: Graph, vertex: Vertex, edge: Edge): TinkerGraph = {
    val g = new TinkerGraph
    graph.getEdges.asScala.foreach((e: Edge) => {
      if (!edge.equals( e)) {
        g.addEdge(e.getId, e.getVertex(OUT), e.getVertex(IN), e.getLabel)
      }
    })

    val vertices = g.getEdges.asScala
      .flatMap(edge => List(edge.getVertex(OUT),edge.getVertex(IN))).toSet + vertex

    vertices.foreach(
      v => {
          val vertex1 = g.addVertex(v.getId)
          vertex1.setProperty("label", v.getProperty("label"))
    })

    g

  }


  private def getSpittedItems(items: String): List[String] =
    items.replace("{", "").replace("}", "").split(',').map(_.trim).toList


  private def getBagFromVertexHTD(vertex: Vertex): Bag = {
    val label: String = vertex.getProperty("label")
    val predicateAndVariables: Array[String] = label.split(" {4}")
    if (predicateAndVariables.length != 2) throw new RuntimeException("Incorrect vertex label.")
    val predicates: List[String] = getSpittedItems(predicateAndVariables(0)).map(_.toLowerCase)
    val atoms: Set[Atom] =
      mapCqAtoms.filter(entry => predicates.contains(entry._1.getIdentifier.toString.toLowerCase)).values.toSet
    val terms: Set[Term] = atoms.flatMap(a=> a.getTerms.asScala)
    // remove __<num>__
    // regular expression
    val atomsRenamed = atoms.map( renameAtom  )
    Bag(atomsRenamed, terms)

  }

  private def getBagFromVertexTD(vertex: Vertex): Bag = {
    val label: String = vertex.getProperty("label")
    val predicateAndVariables: Array[String] = label.split(" {4}")
    if (predicateAndVariables.length != 2) throw new RuntimeException("Incorrect vertex label.")
    val variables: Set[String] = getSpittedItems(predicateAndVariables(1)).map(_.replace("?", "")).toSet
    val atoms: Set[Atom] =
      cqAtoms.filter(atom => {
        val atomVariables =  atom.getTerms.asScala.toList.distinct.sorted.map(_.getIdentifier.toString).toSet
        variables.intersect(atomVariables) == atomVariables  })

    val terms: Set[Term] = variables.map(v=> DefaultTermFactory.instance().createVariable(v))
    Bag(atoms,terms)

  }

  private def renameAtom(atom:Atom) : Atom={
    val pattern = "(__\\d+__)".r
    val newPredicateName =  pattern.replaceAllIn( atom.getPredicate.getIdentifier.toString, "" )
    new DefaultAtom( new Predicate( newPredicateName, atom.getPredicate.getArity), atom.getTerms() )
  }


  /**
    * This method returns the size of the {{{@link TreeDecomposition}}}
    *
    * @return an int value
    */
  def getSize: Int = {

    def getSizeH(t: TreeDecomposition): Int = {
      if (t.children == null || t.children.isEmpty) 1
      else t.children.map(getSizeH).sum + 1
    }

    getSizeH(this)
  }

  def getRoot: Bag = root

  def getChildren: List[TreeDecomposition] = children

  def getCentroid: TreeDecomposition = {
    CentroidDecomposition.getCentroid(this)
  }

  /**
    * Returns an endpoint of this rooted tree. In leaf mode the decomposition is
    * first converted to a path, so this selects a path endpoint.
    */
  def getLeaf: TreeDecomposition = {
    @tailrec
    def findLeaf(t: TreeDecomposition): TreeDecomposition =
      t.children.headOption match {
        case Some(child) => findLeaf(child)
        case None => t
      }

    findLeaf(this)
  }

  private[stypes] def asPathDecomposition: TreeDecomposition = {
    val nodes = mutable.ArrayBuffer.empty[TreeDecomposition]
    val parents = mutable.Map.empty[TreeDecomposition, Option[TreeDecomposition]]

    def collect(node: TreeDecomposition, parent: Option[TreeDecomposition]): Unit = {
      nodes += node
      parents.put(node, parent)
      node.children.foreach(child => collect(child, Some(node)))
    }

    collect(this, None)

    val neighbours = nodes.map { node =>
      node -> (node.children ::: parents(node).toList)
    }.toMap
    val start = nodes.find(node => neighbours(node).size <= 1).get
    val order = mutable.ArrayBuffer.empty[TreeDecomposition]

    def visit(node: TreeDecomposition, previous: Option[TreeDecomposition]): Unit = {
      order += node
      neighbours(node).filterNot(previous.contains).foreach(next => visit(next, Some(node)))
    }

    visit(start, None)

    val positions = order.zipWithIndex.toMap
    val lastPositions = order.map { node =>
      val position = positions(node)
      neighbours(node).map(positions).foldLeft(position)(math.max)
    }
    val nodesEndingAt = order.indices.groupBy(lastPositions).map {
      case (position, indexes) => position -> indexes.map(order)
    }
    val activeNodes = mutable.LinkedHashSet.empty[TreeDecomposition]
    val assignedAtoms = mutable.Set.empty[Atom]
    val pathBags = order.indices.map { position =>
      activeNodes += order(position)
      val atoms = order(position).root.atoms.filter(assignedAtoms.add)
      val bag = Bag(
        atoms,
        activeNodes.flatMap(_.root.variables).toSet
      )
      nodesEndingAt.getOrElse(position, Seq.empty).foreach(node => activeNodes -= node)
      bag
    }

    pathBags.reverse.foldLeft(Option.empty[TreeDecomposition]) { (child, bag) =>
      Some(new TreeDecomposition(mapCqAtoms, bag, child.toList))
    }.get
  }

  @deprecated("Use getCentroid instead.", "")
  def  getSeparator : TreeDecomposition =
    getSplitter(this, this.getSize)

  @deprecated("Use CentroidDecomposition.getCentroid instead.", "") @tailrec
  private def getSplitter(t: TreeDecomposition, rootSize: Int) : TreeDecomposition = {

    @tailrec
    def visitChildren(maxsize:Int, children:List[TreeDecomposition], child:TreeDecomposition  ): TreeDecomposition = children match {
      case  List() => child
      case  x :: xs =>
        val size = x.getSize
        if (size > maxsize) visitChildren(size, xs, x )
        else visitChildren(maxsize, xs, child)
    }
    val tsz= t.getSize
    if ((tsz == rootSize & t.children.size == tsz-1 ) // is already balanced
      || t.getSize <= (rootSize / 2) + 1) {
      t
    }
    else {
      val child: TreeDecomposition = visitChildren(-1, t.children, null)
      getSplitter(child, rootSize)
    }
  }

  def remove(s: TreeDecomposition): TreeDecomposition = {
    val children = this.children.filter(c => c != s ).map(c => c.remove(s)  )
    new TreeDecomposition(mapCqAtoms, root, children )
  }

  def getAllTerms: Set[Term] =  getAllTerms(this.children) ++ this.root.variables

  private def getAllTerms(list: List[TreeDecomposition]) : Set[Term] =  {

    @tailrec
    def doUnion(  list: List[TreeDecomposition], acc: Set[Term]  ): Set[Term] = list match {
      case List() => acc
      case x::xs =>  doUnion( xs, x.root.variables ++ acc ++ getAllTerms( x.children ) )

    }

    if ( list == List() )  Set()
    else doUnion(list, Set() )


  }

  override def toString : String = {
    s"(root: $root, children: $children, mapCqAtoms: $mapCqAtoms, )"
  }


}

object TreeDecomposition{
  def getHyperTreeDecomposition(fileGML: String, fileCQ: String): TreeDecomposition = {
    val pattern: Regex = "(?<=\\()[^)]+(?=\\))".r
    val tf: TermFactory = DefaultTermFactory.instance
    val atoms = Source.fromFile(fileCQ).getLines().map(
      line => {
        val termss = pattern
          .findAllIn(line)
          .flatMap(p => p.split(","))
          .toList.map(_.trim)
        val terms: List[Term] = termss.map(tf.createVariable(_))

        val predicateName = line.split('(').head
        val predicate: Predicate = new Predicate(predicateName, terms.length)

        new DefaultAtom(predicate, terms.asJava)
      }
    )

    val graph: Graph = new TinkerGraph
    val in = new java.io.FileInputStream(fileGML)

    GMLReader.inputGraph(graph, in)
    new TreeDecomposition(atoms.toSet, graph, null, false)

  }

  def getTreeDecomposition(fileGML: String, fileCQWithHead: String): ( TreeDecomposition , List[Variable] ) = {

    val textQueries = Source.fromFile(fileCQWithHead).getLines()
      .map( line  =>  line .replaceAll( "<-", ":-" ).replace("?", "") ).mkString("\n")

    val rules:List[Rule] = GraalIterator.toList(new DlgpParser(textQueries)).map{
      case rule:Rule => rule
    }

    //val atoms = rules.head.getBody.asScala

    val atoms = GraalIterator.toList(rules.head.getBody.iterator()).map(atom => {
      val terms: List[Term] = atom.getTerms.asScala.toList.map(t => DefaultTermFactory.instance().createVariable(t.getIdentifier))
      new DefaultAtom(atom.getPredicate, terms.asJava)
    })


    val graph: Graph = new TinkerGraph
    val in = new java.io.FileInputStream(fileGML)

    GMLReader.inputGraph(graph, in)
    (new TreeDecomposition(atoms.toSet, graph, null, mode = true),
      rules.head.getHead.getTerms.asScala.toList.map(p=> DefaultTermFactory.instance().createVariable( p.getIdentifier)) )
  }
}
