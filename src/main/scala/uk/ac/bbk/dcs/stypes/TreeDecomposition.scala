package uk.ac.bbk.dcs.stypes

import com.tinkerpop.blueprints.Direction.{IN, OUT}
import com.tinkerpop.blueprints.impls.tg.TinkerGraph
import com.tinkerpop.blueprints.{Edge, Graph, Vertex}
import fr.lirmm.graphik.graal.api.core.{Atom, Predicate, Term}
import fr.lirmm.graphik.graal.core.DefaultAtom

import scala.annotation.tailrec
import scala.collection.JavaConverters._

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


  private def getBagFromVertex(vertex: Vertex): Bag = {
    val label: String = vertex.getProperty("label")
    val predicateAndVariables: Array[String] = label.split(" {4}")
    if (predicateAndVariables.length != 2) throw new RuntimeException("Incorrect vertex label.")
    val predicates: List[String] = getSpittedItems(predicateAndVariables(0)).map(_.toLowerCase)
    val atoms: Set[Atom] =
      mapCqAtoms.filter(entry => predicates.contains(entry._1.getIdentifier.toString)).values.toSet
    val terms: Set[Term] = atoms.flatMap(a=> a.getTerms.asScala)
    // remove __<num>__
    // regular expression
    val atomsRenamed = atoms.map( renameAtom  )
    Bag(atomsRenamed, terms)




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

  def getAllTerms() : Set[Term] =  getAllTerms(this.childes) ++ this.root.variables

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
