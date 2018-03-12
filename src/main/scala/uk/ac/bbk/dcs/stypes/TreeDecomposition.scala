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

import com.tinkerpop.blueprints.Direction.{IN, OUT}
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.blueprints.util.io.gml.GMLReader
import com.tinkerpop.blueprints.{Edge, Graph, Vertex}
import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Rule, Term}
import fr.lirmm.graphik.graal.api.factory.TermFactory
import fr.lirmm.graphik.graal.core.DefaultAtom
import fr.lirmm.graphik.graal.core.term.DefaultTermFactory
import fr.lirmm.graphik.graal.io.dlp.DlgpParser

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.io.Source
import scala.reflect.io.File
import scala.util.matching.Regex

/**
  * Created by :
  *   Salvatore Rapisarda
  *   Stanislav Kikot
  * <p>
  * on 30/03/2017.
  */
class TreeDecomposition {

  private var mapCqAtoms: Map[Predicate, Atom] = _
  private var root: Bag = _
  private var childes: List[TreeDecomposition] = _


  def this(cqAtoms: Set[Atom], graph: Graph, v: Vertex, mode: Boolean = false) {
    this()

    // checks preconditions
    if (graph.getVertices.asScala.isEmpty)
      throw new RuntimeException("Vertex cannot be null!!") // Todo: Add proper exception

    this.mapCqAtoms = cqAtoms.map(atom => atom.getPredicate -> atom).toMap
    val vertex: Vertex = if (v != null) {
      v
    } else {
      graph.getVertices.asScala.head
    }

    root =  if (mode) getBagFromVertexTD(vertex) else getBagFromVertexHTD(vertex)


    // TODO:  review this
    this.childes = vertex.getEdges(OUT)
      .asScala.map(edge =>
          new TreeDecomposition(cqAtoms,
                getSubGraph(graph, vertex, edge),
                edge.getVertex(IN), mode))
      .toList

  }

  private[TreeDecomposition] def this(cqAtoms: Map[Predicate, Atom], root: Bag, childes: List[TreeDecomposition]) {
    this()
    this.mapCqAtoms = cqAtoms
    this.root = root
    this.childes = childes
  }


  def getSubGraph(graph: Graph, vertex: Vertex, edge: Edge): TinkerGraph = {
    val g = new TinkerGraph
    graph.getVertices.asScala.foreach(
      v => {
        if (!vertex.equals(v) ) {
          val vertex1 = g.addVertex(v.getId)
          vertex1.setProperty("label", v.getProperty("label"))
        }
    })

    graph.getEdges.asScala.foreach((e: Edge) => {
      if (!edge.equals( e))
        g.addEdge(e.getId, e.getVertex(OUT), e.getVertex(IN), e.getLabel)
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
    val variables: List[String] = getSpittedItems(predicateAndVariables(1)).map(_.toLowerCase.replace("?", ""))
    val atoms: Set[Atom] =
      mapCqAtoms.filter(atom =>
        variables.distinct.sorted.containsSlice(
          atom._2.getTerms.asScala.toList.distinct.sorted.map(_.getIdentifier.toString))).values.toSet
    val terms: Set[Term] = variables.map(v=> DefaultTermFactory.instance().createVariable(v)).toSet
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
      if (t.childes == null || t.childes.isEmpty) 1
      else t.childes.map(getSizeH).sum + 1
    }

    getSizeH(this)
  }

  def getRoot: Bag = root

  def getChildes: List[TreeDecomposition] = childes

  def  getSeparator : TreeDecomposition = getSplitter(this, this.getSize)

  @tailrec
  private def getSplitter(t: TreeDecomposition, rootSize: Int) : TreeDecomposition = {

    @tailrec
    def visitChildes( maxsize:Int, childes:List[TreeDecomposition], child:TreeDecomposition  ): TreeDecomposition = childes match {
      case  List() => child
      case  x :: xs =>
        val size = x.getSize
        if (size > maxsize) visitChildes(size, xs, x )
        else visitChildes(maxsize, xs, child)
    }

    if (t.getSize <= (rootSize / 2) + 1) t
    else {
      val child: TreeDecomposition = visitChildes(-1, t.childes, null)
      getSplitter(child, rootSize)
    }

  }

  def remove(s: TreeDecomposition): TreeDecomposition = {
    val childes = this.childes.filter( c => c != s ).map( c => c.remove(s)  )
    new TreeDecomposition(mapCqAtoms, root, childes )
  }

  def getAllTerms: Set[Term] =  getAllTerms(this.childes) ++ this.root.variables

  private def getAllTerms(list: List[TreeDecomposition]) : Set[Term] =  {

    @tailrec
    def doUnion(  list: List[TreeDecomposition], acc: Set[Term]  ): Set[Term] = list match {
      case List() => acc
      case x::xs =>  doUnion( xs, x.root.variables ++ acc ++ getAllTerms( x.childes ) )

    }

    if ( list == List() )  Set()
    else doUnion(list, Set() )


  }

  override def toString : String = {
    s"(root: $root, childes: $childes, mapCqAtoms: $mapCqAtoms, )"
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
        val terms = termss.map(tf.createVariable(_))

        val predicateName = line.split('(').head
        val predicate: Predicate = new Predicate(predicateName, terms.length)

        new DefaultAtom(predicate, terms: _*)
      }
    )

    val graph: Graph = new TinkerGraph
    val in = File(fileGML).inputStream()

    GMLReader.inputGraph(graph, in)
    new TreeDecomposition(atoms.toSet, graph, null, false)

  }

  def getTreeDecomposition(fileGML: String, fileCQWithHead: String): ( TreeDecomposition , List[Term] ) = {

    val textQueries = File(fileCQWithHead).lines()
      .map( line  =>  line .replaceAll( "<-", ":-" ).replace("?", "") ).mkString("\n")

    val rules:List[Rule] = new DlgpParser(textQueries).asScala.toList.map{
      case rule:Rule => rule
    }

    //val atoms = rules.head.getBody.asScala

    val atoms = rules.head.getBody.asScala.map(atom => {
      val terms: List[Term] = atom.getTerms.asScala.toList.map(t => DefaultTermFactory.instance().createVariable(t.getIdentifier))
      new DefaultAtom(atom.getPredicate, terms.asJava)
    })


    val graph: Graph = new TinkerGraph
    val in = File(fileGML).inputStream()

    GMLReader.inputGraph(graph, in)
    (new TreeDecomposition(atoms.toSet, graph, null, mode = true), rules.head.getHead.getTerms.asScala.toList)

  }
}
