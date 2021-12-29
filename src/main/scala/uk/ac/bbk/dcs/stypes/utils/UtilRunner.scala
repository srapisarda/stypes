package uk.ac.bbk.dcs.stypes.utils

object UtilRunner extends App {

  println(s"args.length: ${args.length}")

  if(args.length > 1) {
      println(args(0))
      println("***********")
      println(args(1))
  }


}
