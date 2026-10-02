package uk.ac.bbk.dcs.stypes.utils

object UtilRunner {

  def main(args: Array[String]): Unit = {
    println(s"args.length: ${args.length}")

    if(args.length > 1) {
      println(args(0))
      println("***********")
      println(args(1))
    }
  }
}
