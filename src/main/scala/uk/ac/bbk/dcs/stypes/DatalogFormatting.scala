package uk.ac.bbk.dcs.stypes

private[stypes] object DatalogFormatting {
  private val graalPredicateArity = """\\\d+""".r

  def withoutPredicateArities(value: String): String =
    graalPredicateArity.replaceAllIn(value, "")
}
