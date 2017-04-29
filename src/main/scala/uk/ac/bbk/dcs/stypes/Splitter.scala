package uk.ac.bbk.dcs.stypes

import scala.collection.mutable

/**
  * Created by rapissal on 28/04/2017.
  */
class Splitter() {

  private[stypes] var root: TreeDecomposition = _
  private[stypes] var children: List[Splitter] = _

  def this(t: TreeDecomposition) {
    this()
    root = t
    val splitter = root.getSplitter
    this.children = splitter.getChildes.map(new Splitter(_)) //.  .size // .map(  t-> new Splitter(t))
    if (splitter != root) {
      val secondChildren: TreeDecomposition = t.remove(splitter)
      children = new Splitter(secondChildren) :: children
    }
  }


  override def toString: String = {
    s"(root: $root, children: $children)"
  }

}
