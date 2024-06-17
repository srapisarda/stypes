package uk.ac.bbk.dcs.stypes

import com.typesafe.scalalogging.Logger

import scala.collection.mutable


object CentroidDecomposition {
  private val logger = Logger(this.getClass)

  def getCentroid(vertex: TreeDecomposition, visited: Set[TreeDecomposition] = Set(), subtreeSize: mutable.Map[TreeDecomposition, Int] = mutable.Map()): TreeDecomposition = {

    def dfs(vertex: TreeDecomposition, parent: TreeDecomposition): Int = {
      var size = 1
      for (to <- vertex.getChildren) {
        if (!subtreeSize.contains(to) && parent != to && to.getRoot.atoms.nonEmpty)
          size += dfs(to, vertex)
      }
      logger.debug(s"Vertex ${vertex.hashCode()}: ${vertex.getRoot.atoms}, size: $size")
      subtreeSize.put(vertex, size)
      size
    }

    @scala.annotation.tailrec
    def findCentroid(vertex: TreeDecomposition, parent: TreeDecomposition, visited: Set[TreeDecomposition], n: Int): TreeDecomposition = {
      var heaviest: Option[TreeDecomposition] = None
      var isCentroid = true

      for (to <- vertex.getChildren) {
        if (!visited.contains(to) && parent != to) {
          if (subtreeSize(to) > n / 2)
            isCentroid = false

          if (heaviest.isEmpty || subtreeSize(heaviest.get) < subtreeSize(to))
            heaviest = Some(to)
        }
      }

      if (isCentroid && n - subtreeSize(vertex) <= n / 2)
        vertex
      else
        findCentroid(heaviest.get, vertex, visited + vertex, n)
    }

    dfs(vertex, null)
    findCentroid(vertex, null, visited, subtreeSize(vertex))
  }
}
