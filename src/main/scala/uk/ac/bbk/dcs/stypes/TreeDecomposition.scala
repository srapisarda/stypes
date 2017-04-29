package uk.ac.bbk.dcs.stypes

import com.tinkerpop.blueprints.Direction.{IN, OUT}
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.blueprints.{Edge, Graph, Vertex}
import fr.lirmm.graphik.graal.api.core.{Atom, Predicate}

import scala.collection.JavaConverters._
import scala.collection.mutable

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


  def this(cqAtoms: Set[Atom], graph: Graph, v: Vertex) {
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

    root = getBagFromVertex(vertex)


    // TODO:  review this
    this.childes = vertex.getEdges(OUT)
      .asScala.map(edge =>
          new TreeDecomposition(cqAtoms,
                getSubGraph(graph, vertex, edge),
                edge.getVertex(IN)))
      .toList

  }

  private[TreeDecomposition] def this(cqAtoms: Map[Predicate, Atom], root: Bag, childes: List[TreeDecomposition]) {
    this()
    this.mapCqAtoms = cqAtoms
    this.root = root
    this.childes = childes
  }


  def getSubGraph(graph: Graph, vertex: Vertex, edge: Edge) = {
    val g = new TinkerGraph
    graph.getVertices.asScala.foreach(
      v => {
        if (!vertex.equals(v) ) {
          var vertex1 = g.addVertex(v.getId)
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
    items.replace("{", "").replace("}", "").split(", ").toList


  private def getBagFromVertex(vertex: Vertex): Bag = {
    val label: String = vertex.getProperty("label")
    val split: Array[String] = label.split(" {4}")
    if (split.length != 2) throw new RuntimeException("Incorrect vertex label.")
    val predicates: List[String] = getSpittedItems(split(0)).map(_.toLowerCase)
    val atoms: Set[Atom] =
      mapCqAtoms.filter(entry => predicates.contains(entry._1.getIdentifier.toString)).values.toSet

    Bag(atoms)
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


  def  getSplitter : TreeDecomposition = getSplitter(this, this.getSize)

  // todo: make it tailrec
  private def getSplitter(t: TreeDecomposition, rootSize: Int) : TreeDecomposition =
    if (t.getSize <= (rootSize / 2) + 1) t
    else {
      var maxsize = -1
      var child: TreeDecomposition = null
      for (c <- t.getChildes) {
        val size = c.getSize
        if (size > maxsize) {
          child = c
          maxsize = size
        }
      }
      getSplitter(child, rootSize)
    }


  def remove(s: TreeDecomposition): TreeDecomposition = {

//    def removeH(s: TreeDecomposition, childes:List[TreeDecomposition],
//                acc:List[TreeDecomposition]): List[TreeDecomposition] = childes match {
//      case List() => acc
//      case x::xs => if ( x !=  s) removeH(s, xs, x::acc ) else  removeH(s, xs, acc )
//    }

 //    removeH(s, this.childes )

    val ret:mutable.MutableList[TreeDecomposition] = new mutable.MutableList[TreeDecomposition]
    for (c <- this.getChildes) {
      if (c != s)  ret += c.remove(s)
    }



    new TreeDecomposition(mapCqAtoms, root, ret.toList )
  }

  override def toString= {
    s"(mapCqAtoms: $mapCqAtoms, root: $root, childes: $childes)"
  }

}
