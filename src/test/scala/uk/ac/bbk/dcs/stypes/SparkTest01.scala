package uk.ac.bbk.dcs.stypes

package uk.ac.bbk.dcs.spark.ndl

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSpec

/**
  * Created by Salvatore Rapisarda on 24/12/2017.
  */
class SparkTest01 extends FunSpec {
  private val pathToBenchmarkNDL_SQL = "src/main/resources/benchmark/NDL-SQL"
  private val config = new SparkConf().setAppName("SparkTest03").setMaster("local[2]").set("spark.executor.memory", "1g")
  private val sc = SparkContext.getOrCreate(config)

  sc.setCheckpointDir(s"$pathToBenchmarkNDL_SQL/data")

  private val a = sc.textFile(s"$pathToBenchmarkNDL_SQL/data/20mb-a.txt")
  private val b = sc.textFile(s"$pathToBenchmarkNDL_SQL/data/20mb-b.txt")
  private val r = sc.textFile(s"$pathToBenchmarkNDL_SQL/data/20mb-r.txt").map(
    line => {
      val row = line.split(',')
      (row(0), row(1))
    })
  private val s = sc.textFile(s"$pathToBenchmarkNDL_SQL/data/20mb-s.txt").map(
    line => {
      val row = line.split(',')
      (row(0), row(1))
    })
  private val r50 = sc.textFile(s"$pathToBenchmarkNDL_SQL/data/20mb-r-50.txt").map(
    line => {
      val row = line.split(',')
      (row(0), row(1))
    })

  private def myJoin(firstRelation: org.apache.spark.rdd.RDD[(String, String)], secondRelation: org.apache.spark.rdd.RDD[(String, String)]) = {
    firstRelation.map(t => (t._2, t._1)).join(secondRelation).values
  }

  describe("ndl rewriting") {

    it("should make a self join ") {
      //P_7_9(X,Y) :- R(X,Z), R(Z,Y).
      r50.foreach(println)

      val selfJoin: RDD[(String, String)] = myJoin(r, r)

      println( s"P_7_9(X,Y) :- R(X,Z), R(Z,Y). count: ${selfJoin.count}")
//      arr.foreach(p =>
//        println(s"R(${p._1}, ${p._2})")
//      )
    }


    it ("should make join on "){
      //P_5_7(X,Y) :- R(X,Z), S(Z,Y).
      //P_5_7(X,X) :- B(X).

      val P_5_7 = b.map(x => (x, x)).union(myJoin(r, s))
      println(s"P_5_7(X,Y) :- R(X,Z), S(Z,Y).\n" +
          s"P_5_7(X,X) :- B(X).\n" +
        s"count: ${P_5_7.count}")
    }



    it("should run the  following tests") {


      lazy val aMapped =  a.map(x => (x, x)).cache()

      lazy val bMapped = b.map(x => (x, x)).cache()

      //P_7_9(X,Y) :- R(X,Z), R(Z,Y).

      lazy val P_7_9 = myJoin(r, r).cache()

      //P_5_7(X,Y) :- R(X,Z), S(Z,Y).
      //P_5_7(X,X) :- B(X).

      lazy val P_5_7 = b.map(x => (x, x)).union(myJoin(r, s)).cache()

      //P_1_3(X,Y) :- R(X,Z), S(Z,Y).
      //P_1_3(X,X) :- B(X).

      lazy val P_1_3 =  P_5_7 // b.map(x => (x, x)).union(myJoin(r, s)).cache()

      //P_4_7(X,Y) :- S(X,Z), P_5_7(Z,Y).
      //P_4_7(X,Y) :- A(X), S(X,Y).

      lazy val P_4_7 = a.map(x => (x, x)).union(myJoin(s, P_5_7)).cache()

      //P_9_11(X,Y) :- S(X,Z), R(Z,Y).
      //P_9_11(X,X) :- A(X).

      lazy val P_9_11 = a.map(x => (x, x)).union(myJoin(s, r)).cache()

      //P_13_15(X,Y) :- S(X,Z), R(Z,Y).
      //P_13_15(X,X) :- A(X).

      lazy val P_13_15 = P_9_11 // a.map(x => (x, x)).union(myJoin(s, r)).cache()

      //P_11_13(X,Y) :- R(X,Z), S(Z,Y).
      //P_11_13(X,X) :- B(X).

      lazy val P_11_13 = P_5_7 // b.map(x => (x, x)).union(myJoin(r, s)).cache()

      //P_11_15(X,Y) :- P_11_13(X,Z), P_13_15(Z,Y).

      lazy val P_11_15 = myJoin(P_11_13, P_13_15).cache()

      //P_8_11(X,Y) :- R(X,Z), P_9_11(Z,Y).
      //P_8_11(X,Y) :- B(X), R(X,Y).

      lazy val P_8_11 = myJoin(r, P_9_11).union(myJoin(bMapped, r)).cache()

      //P_8_15(X,Y) :- P_8_11(X,Z), P_11_15(Z,Y).

      lazy val P_8_15 = myJoin(P_8_11, P_11_15).cache()

      //P_3_5(X,Y) :- R(X,Z), S(Z,Y).
      //P_3_5(X,X) :- B(X).

      lazy val P_3_5 = bMapped.union(myJoin(r, s)).cache()

      //P_3_7(X,Y) :- P_3_5(X,Z), P_5_7(Z,Y).
      //P_3_7(X,Y) :- R(X,Z), A(Z), S(Z,Y).

      lazy val P_3_7 = myJoin(P_3_5, P_5_7).union(myJoin(myJoin(r, aMapped), s)).cache()

      //P_4_6(X,Y) :- S(X,Z), R(Z,Y).
      //P_4_6(X,X) :- A(X).

      lazy val P_4_6 = a.map(x => (x, x)).union(myJoin(s, r)).cache()

      //P_3_6(X,Y) :- R(X,Z), P_4_6(Z,Y).
      //P_3_6(X,Y) :- B(X), R(X,Y).

      val P_3_6 = myJoin(r, P_4_6).union(myJoin(bMapped, r)).cache()

      //P_7_11(X,Y) :- P_7_9(X,Z), P_9_11(Z,Y).
      //P_7_11(X,Y) :- R(X,Z), B(Z), R(Z,Y).

      val P_7_11 = myJoin(P_7_9, P_9_11).union(myJoin(myJoin(r,bMapped ), r)).cache()

      //P_7_15(X,Y) :- P_7_11(X,Z), P_11_15(Z,Y).

      val P_7_15 = myJoin(P_7_11, P_11_15).cache()

      //P_0_2(X,Y) :- R(X,Z), R(Z,Y).

      val P_0_2 = P_7_9 // myJoin(r, r).cache()

      //P_0_3(X,Y) :- R(X,Z), P_1_3(Z,Y).

      val P_0_3 = myJoin(r, P_1_3).cache()

      //P_0_6(X,Y) :- P_0_3(X,Z), P_3_6(Z,Y).
      //P_0_6(X,Y) :- P_0_2(X,Z), A(Z), P_4_6(Z,Y).

      lazy val p_0_2_aMapped = myJoin(P_0_2, aMapped).cache()

      val P_0_6 = myJoin(P_0_3, P_3_6).union(myJoin(p_0_2_aMapped, P_4_6)).cache()

      //P_0_7(X,Y) :- P_0_3(X,Z), P_3_7(Z,Y).
      //P_0_7(X,Y) :- P_0_2(X,Z), A(Z), P_4_7(Z,Y).

      val P_0_7 = myJoin(P_0_3, P_3_7).union(myJoin(p_0_2_aMapped, P_4_7)).cache()

      //P_0_15(X,Y) :- P_0_7(X,Z), P_7_15(Z,Y).
      //P_0_15(X,Y) :- P_0_6(X,Z), A(Z), P_8_15(Z,Y).

      val P_0_15 = myJoin(P_0_7, P_7_15).union(myJoin(myJoin(P_0_6, aMapped), P_8_15))

      println(s"P_0_15.distinct.count: ${P_0_15.count}")
    }
  }
}
