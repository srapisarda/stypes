package uk.ac.bbk.dcs.stypes

import fr.lirmm.graphik.util.stream.CloseableIterator

private[stypes] object GraalIterator {
  def toList[A](iterator: CloseableIterator[A]): List[A] = {
    val result = List.newBuilder[A]
    try {
      while (iterator.hasNext) result += iterator.next()
      result.result()
    } finally {
      iterator.close()
    }
  }
}
