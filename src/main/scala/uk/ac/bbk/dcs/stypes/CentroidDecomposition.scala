package uk.ac.bbk.dcs.stypes

import com.typesafe.scalalogging.Logger

import scala.collection.mutable


object CentroidDecomposition {
  private val logger = Logger(this.getClass)

  case class TreeInfo(size: Int, degree: Int = 0)

  def getCentroid(vertex: TreeDecomposition, visited: Set[TreeDecomposition] = Set(),
                  subtreeSize: mutable.Map[TreeDecomposition, TreeInfo] = mutable.Map()): TreeDecomposition = {

    def dfs(vertex: TreeDecomposition, parent: TreeDecomposition): Int = {
      var size = 1
      for (to <- vertex.getChildren) {
        if (!subtreeSize.contains(to) && parent != to && to.getRoot.atoms.nonEmpty)
          size += dfs(to, vertex)
      }
      subtreeSize.contains(parent)
      val degree = if (parent == null) 0  else vertex.getChildren.size + 1
      // logger.debug(s"Vertex ${vertex.hashCode()}: ${vertex.getRoot.atoms}, size: $size, degree: $degree")
      subtreeSize.put(vertex, TreeInfo(size, degree))
      size
    }

    @scala.annotation.tailrec
    def findCentroid(vertex: TreeDecomposition, parent: TreeDecomposition, visited: Set[TreeDecomposition], n: Int): TreeDecomposition = {
      var heaviest: Option[TreeDecomposition] = None
      var isCentroid = true

      for (to <- vertex.getChildren) {
        if (!visited.contains(to) && parent != to) {
          if ( subtreeSize.contains(to) && subtreeSize(to).size > n / 2  )
            isCentroid = false

          if (heaviest.isEmpty || subtreeSize(heaviest.get).size < subtreeSize(to).size)
            heaviest = Some(to)
        }
      }

      if ((isCentroid && n - subtreeSize(vertex).size <= n / 2)
        || subtreeSize(vertex).degree > 2
      )
        vertex
      else
        findCentroid(heaviest.get, vertex, visited + vertex, n)
    }

    dfs(vertex, null)
    findCentroid(vertex, null, visited, subtreeSize(vertex).size)
  }
}
