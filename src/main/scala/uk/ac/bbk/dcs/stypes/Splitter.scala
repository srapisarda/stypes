package uk.ac.bbk.dcs.stypes

/**
  * Created by
  * Salvatore Rapisarda
  * Stanislav Kikot
  * on 28/04/2017.
  */
case class Splitter(root: TreeDecomposition) {
  val separator: TreeDecomposition = root.getSeparator
  val children: List[Splitter] = {
    val directChildren = separator.getChildes.map(Splitter)
    if (separator != root) {
      val rootGeneratedChild: TreeDecomposition = root.remove(separator)
      Splitter(rootGeneratedChild) :: directChildren
    }
    else directChildren
  }

  def getSplittingVertex: Bag = separator.getRoot

  override def toString: String = {
    s"(root: $root, children: $children)"
  }

}
