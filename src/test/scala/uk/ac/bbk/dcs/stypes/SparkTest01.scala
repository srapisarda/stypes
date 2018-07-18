package uk.ac.bbk.dcs.stypes

/*
 * #%L
 * STypeS
 * %%
 * Copyright (C) 2017 - 2021 Birkbeck University of London
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSpec

/**
  * Created by Salvatore Rapisarda on 24/12/2017.
  */
class SparkTest01 extends FunSpec {
  private val pathToBenchmarkNDL_SQL = "src/test/resources/benchmark/NDL-SQL"
  private val config = new SparkConf().setAppName("SparkTest03").setMaster("local[4]")//.set("spark.executor.memory", "1g")
  private val sc = SparkContext.getOrCreate(config)

  sc.setCheckpointDir(s"$pathToBenchmarkNDL_SQL/data")

  private val a = sc.textFile(s"$pathToBenchmarkNDL_SQL/data/20mb-a.txt")
  private val b = sc.textFile(s"$pathToBenchmarkNDL_SQL/data/20mb-b.txt")
  private val r = sc.textFile(s"$pathToBenchmarkNDL_SQL/data/20mb-r.txt").map(
    line => {
      val row = line.split(',')
      (row(0), row(1))
    }).persist(StorageLevel.MEMORY_AND_DISK)
  private val s = sc.textFile(s"$pathToBenchmarkNDL_SQL/data/20mb-s.txt").map(
    line => {
      val row = line.split(',')
      (row(0), row(1))
    }).persist(StorageLevel.MEMORY_AND_DISK)
  private val r50 = sc.textFile(s"$pathToBenchmarkNDL_SQL/data/20mb-r-50.txt").map(
    line => {
      val row = line.split(',')
      (row(0), row(1))
    }).persist(StorageLevel.MEMORY_AND_DISK)

  private def myJoin(firstRelation: org.apache.spark.rdd.RDD[(String, String)],
                     secondRelation: org.apache.spark.rdd.RDD[(String, String)]) = {
    firstRelation.map(t => (t._2, t._1)).join(secondRelation).values
  }

  describe("Executing SPARK NDL rewriting") {

    ignore("should make a self join SPARK RDD Test ") {
      //P_7_9(X,Y) :- R(X,Z), R(Z,Y).
      r50.foreach(println)

      val selfJoin = myJoin(r, r)

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
    lazy val aMapped =  a.map(x => (x, x)).persist(StorageLevel.MEMORY_AND_DISK)

    lazy val bMapped = b.map(x => (x, x)).persist(StorageLevel.MEMORY_AND_DISK)


    ignore("should run the following SPARK SQL Test") {

      val sparkSession = SparkSession.builder.appName("Vlad-Query-20mb-4core")
        .master("local[4]")
        .config("spark.eventLog.enabled", value = true)
        .config("spark.eventLog.dir", "logs").getOrCreate

      sparkSession.conf.set("spark.ui.enabled", value = true)

      val customSchema_1 = new StructType(Array[StructField](
        StructField("X", DataTypes.LongType, nullable = false, Metadata.empty)
      ))

      val customSchema_2 = new StructType(Array[StructField](
        StructField("X", DataTypes.LongType, nullable = false, Metadata.empty),
        StructField("Y", DataTypes.LongType, nullable = false, Metadata.empty)
      ))

      val da = sparkSession.read.option("header", "true").schema(customSchema_1)
        .csv(s"$pathToBenchmarkNDL_SQL/data/20mb-a.csv")
      da.createOrReplaceTempView("a")

      val db = sparkSession.read.option("header", "true").schema(customSchema_1)
        .csv(s"$pathToBenchmarkNDL_SQL/data/20mb-b.csv")
      db.createOrReplaceTempView("b")

      val dr = sparkSession.read.option("header", "true").schema(customSchema_2)
        .csv(s"$pathToBenchmarkNDL_SQL/data/20mb-r.csv")
      dr.createOrReplaceTempView("r")

      val ds = sparkSession.read.option("header", "true").schema(customSchema_2)
        .csv(s"$pathToBenchmarkNDL_SQL/data/20mb-s.csv")
      ds.createOrReplaceTempView("s")

      //<P-0-1> (?X,?Y) :- <R> (?X, ?Y) .

      val p0_1 = sparkSession.sql("SELECT * FROM r")
      p0_1.createOrReplaceTempView("P0_1")

      //<P-1-2> (?X,?Y) :- <R> (?X, ?Y) .

      val p1_2 = sparkSession.sql("SELECT * FROM r")
      p1_2.createOrReplaceTempView("P1_2")

      //<P-2-3> (?X,?Y) :- <S> (?X, ?Y) .

      val p2_3 = sparkSession.sql("SELECT * FROM s")
      p2_3.createOrReplaceTempView("P2_3")

      //<P-3-4> (?X,?Y) :- <R> (?X, ?Y) .

      val p3_4 = sparkSession.sql("SELECT * FROM r")
      p3_4.createOrReplaceTempView("P3_4")

      //<P-4-5> (?X,?Y) :- <S> (?X, ?Y) .

      val p4_5 = sparkSession.sql("SELECT * FROM s")
      p4_5.createOrReplaceTempView("P4_5")

      //<P-6-7> (?X,?Y) :- <S> (?X, ?Y) .

      val p6_7 = sparkSession.sql("SELECT * FROM s")
      p6_7.createOrReplaceTempView("P6_7")

      //<P-7-8> (?X,?Y) :- <R> (?X, ?Y) .

      val p7_8 = sparkSession.sql("SELECT * FROM r")
      p7_8.createOrReplaceTempView("P7_8")

      //<P-8-9> (?X,?Y) :- <R> (?X, ?Y) .

      val p8_9 = sparkSession.sql("SELECT * FROM r")
      p8_9.createOrReplaceTempView("P8_9")

      //<P-9-10> (?X,?Y) :- <S> (?X, ?Y) .

      val p9_10 = sparkSession.sql("SELECT * FROM s")
      p9_10.createOrReplaceTempView("P9_10")

      //<P-9-11> (?X,?X) :- <A>(?X).

      //    var p9_11 = sparkSession.sql("SELECT a.X as X, a.X as Y FROM a")
      //    p9_11.createOrReplaceTempView("P9_11")

      //<P-10-11> (?X,?Y) :- <R> (?X, ?Y) .

      val p10_11 = sparkSession.sql("SELECT * FROM r")
      p10_11.createOrReplaceTempView("P10_11")

      //<P-9-11> (?X,?Y) :- <P-9-10> (?X, ?Z),<P-10-11> (?Z, ?Y).

      val p9_11 = sparkSession.sql("SELECT P9_10.X as X, P10_11.Y as Y \n" +
        "FROM P9_10, P10_11 WHERE P9_10.Y = P10_11.X \n" +
        "UNION ALL \n" +
        "SELECT a.X as X, a.X as Y FROM a")

      p9_11.createOrReplaceTempView("P9_11")

      //<P-1-3> (?X,?X) :- <B>(?X).
      //<P-1-3> (?X,?Y) :- <P-1-2> (?X, ?Z),<P-2-3> (?Z, ?Y).

      val p1_3 = sparkSession.sql("SELECT b.X as X, b.X as Y FROM b UNION \n" +
        "ALL SELECT P1_2.X AS X, P2_3.Y AS Y FROM P1_2, P2_3 WHERE P1_2.Y = P2_3.X")
      p1_3.createOrReplaceTempView("P1_3")

      //<P-0-3> (?X,?Y) :- <P-0-1> (?X, ?Z),<P-1-3> (?Z, ?Y).

      val p0_3 = sparkSession.sql("SELECT P0_1.X as X, P1_3.Y as Y FROM P0_1, P1_3 WHERE P0_1.Y = P1_3.X")
      p0_3.createOrReplaceTempView("P0_3")

      //<P-3-5> (?X,?Y) :- <P-1-3> (?X, ?Y) .

      val p3_5 = sparkSession.sql("SELECT * FROM P1_3")
      p3_5.createOrReplaceTempView("P3_5")

      //<P-5-7> (?X,?Y) :- <P-1-3> (?X, ?Y) .

      val p5_7 = sparkSession.sql("SELECT * FROM P1_3")
      p5_7.createOrReplaceTempView("P5_7")

      //<P-3-7> (?X,?Y) :- <P-3-5> (?X, ?Z),<P-5-7> (?Z, ?Y).
      //<P-3-7> (?X,?Y) :- <P-3-4>(?X, ?Z), <A>(?Z),<P-6-7> (?Z, ?Y).

      val p3_7 = sparkSession.sql("SELECT P3_5.X as X, P5_7.Y as Y FROM \n" +
        "P3_5, P5_7 WHERE P3_5.Y = P5_7.X UNION ALL SELECT P3_4.X AS X, \n" +
        "P6_7.Y AS Y FROM P3_4, a, P6_7 WHERE P3_4.Y = a.X AND P6_7.X = a.X")
      p3_7.createOrReplaceTempView("P3_7")

      //<P-0-2> (?X,?Y) :- <P-0-1> (?X, ?Z),<P-1-2> (?Z, ?Y).

      val p0_2 = sparkSession.sql("SELECT P0_1.X as X, P1_2.Y as Y FROM P0_1, P1_2 WHERE P0_1.Y = P1_2.X")
      p0_2.createOrReplaceTempView("P0_2")

      //<P-4-6> (?X,?Y) :- <P-9-11> (?X, ?Y) .

      val p4_6 = sparkSession.sql("SELECT * FROM P9_11")
      p4_6.createOrReplaceTempView("P4_6")

      //<P-4-7> (?X,?Y) :- <A>(?X), <P-6-7>(?X, ?Y) .
      //<P-4-7> (?X,?Y) :- <P-4-5> (?X, ?Z),<P-5-7> (?Z, ?Y).

      val p4_7 = sparkSession.sql("SELECT a.X as X, P6_7.Y as Y FROM a, \n" +
        "P6_7 WHERE a.X = P6_7.X UNION ALL SELECT P4_5.X as X, P5_7.Y as Y \n" +
        "FROM P4_5, P5_7 WHERE P4_5.Y = P5_7.X")
      p4_7.createOrReplaceTempView("P4_7")

      //<P-0-7> (?X,?Y) :- <P-0-3> (?X, ?Z),<P-3-7> (?Z, ?Y).
      //<P-0-7> (?X,?Y) :- <P-0-2>(?X, ?Z), <A>(?Z),<P-4-7> (?Z, ?Y).

      val p0_7 = sparkSession.sql("SELECT P0_3.X as X, P3_7.Y as Y FROM \n" +
        "P0_3, P3_7 WHERE P0_3.Y = P3_7.X UNION ALL SELECT P0_2.X as X, \n" +
        "P4_7.Y as Y FROM P0_2, P4_7, a WHERE P0_2.Y = a.X AND P4_7.X = a.X")
      p0_7.createOrReplaceTempView("P0_7")

      //<P-7-9> (?X,?Y) :- <P-0-2> (?X, ?Y) .

      val p7_9 = sparkSession.sql("SELECT * FROM P0_2")
      p7_9.createOrReplaceTempView("P7_9")

      //<P-7-11> (?X,?Y) :- <P-7-9> (?X, ?Z),<P-9-11> (?Z, ?Y).
      //<P-7-11> (?X,?Y) :- <P-7-8>(?X, ?Z), <B>(?Z),<P-10-11> (?Z, ?Y).

      val p7_11 = sparkSession.sql("SELECT P7_9.X as X, P9_11.Y as Y FROM \n" +
        "P7_9, P9_11 WHERE P7_9.Y = P9_11.X UNION ALL SELECT P7_8.X AS X, \n" +
        "P10_11.Y AS Y FROM P7_8, b, P10_11 WHERE P7_8.Y = P10_11.X AND \n" +
        "P7_8.Y = b.X ")
      p7_11.createOrReplaceTempView("P7_11")

      //<P-8-11> (?X,?Y) :- <P-8-9> (?X, ?Z),<P-9-11> (?Z, ?Y).
      //<P-8-11> (?X,?Y) :- <B>(?X),<P-10-11>(?X, ?Y).

      val p8_11 = sparkSession.sql("SELECT P8_9.X as X, P9_11.Y as Y FROM \n" +
        "P8_9, P9_11 WHERE P8_9.Y = P9_11.X UNION ALL SELECT P10_11.X as X, \n" +
        "P10_11.Y as Y FROM P10_11, b WHERE P10_11.X = b.X ")
      p8_11.createOrReplaceTempView("P8_11")

      //<P-3-6> (?X,?Y) :- <P-8-11> (?X, ?Y) .

      val p3_6 = sparkSession.sql("SELECT * FROM P8_11")
      p3_6.createOrReplaceTempView("P3_6")

      //<P-13-15> (?X,?Y) :- <P-9-11> (?X, ?Y) .

      val p13_15 = sparkSession.sql("SELECT * FROM P9_11")
      p13_15.createOrReplaceTempView("P13_15")

      //<P-11-13> (?X,?Y) :- <P-1-3> (?X, ?Y) .

      val p11_13 = sparkSession.sql("SELECT * FROM P1_3")
      p11_13.createOrReplaceTempView("P11_13")

      //<P-11-15> (?X,?Y) :- <P-11-13> (?X, ?Z),<P-13-15> (?Z, ?Y).

      val p11_15 = sparkSession.sql("SELECT P11_13.X as X, P13_15.Y as Y \n" +
        "FROM P11_13, P13_15 WHERE P11_13.Y = P13_15.X ")
      p11_15.createOrReplaceTempView("P11_15")

      //<P-7-15> (?X,?Y) :- <P-7-11> (?X, ?Z),<P-11-15> (?Z, ?Y).

      val p7_15 = sparkSession.sql("SELECT P7_11.X as X, P11_15.Y as Y \n" +
        "FROM P7_11, P11_15 WHERE P7_11.Y = P11_15.X ")
      p7_15.createOrReplaceTempView("P7_15")

      //<P-8-15> (?X,?Y) :- <P-8-11> (?X, ?Z),<P-11-15> (?Z, ?Y).

      val p8_15 = sparkSession.sql("SELECT P8_11.X as X, P11_15.Y as Y \n" +
        "FROM P8_11, P11_15 WHERE P8_11.Y = P11_15.X ")
      p8_15.createOrReplaceTempView("P8_15")

      //<P-0-6> (?X,?Y) :- <P-0-3> (?X, ?Z),<P-3-6> (?Z, ?Y).
      //<P-0-6> (?X,?Y) :- <P-0-2>(?X, ?Z), <A>(?Z),<P-4-6> (?Z, ?Y).

      val p0_6 = sparkSession.sql("SELECT P0_3.X as X, P3_6.Y as Y FROM \n" +
        "P0_3, P3_6 WHERE P0_3.Y = P3_6.X \n" +
        "UNION ALL \n" +
        "SELECT P0_2.X as X, P4_6.Y as Y FROM P0_2, P4_6, a WHERE P0_2.Y = a.X AND P4_6.X = a.X")
      p0_6.createOrReplaceTempView("P0_6")

      //<P-0-15> (?X,?Y) :- <P-0-7> (?X, ?Z),<P-7-15> (?Z, ?Y).
      //<P-0-15> (?X,?Y) :- <P-0-6>(?X, ?Z), <A>(?Z),<P-8-15> (?Z, ?Y).

      val p0_15 = sparkSession.sql("SELECT P0_7.X as X, P7_15.Y as Y FROM \n" +
        "P0_7, P7_15 WHERE P0_7.Y = P7_15.X UNION ALL SELECT P0_6.X AS X, \n" +
        "P8_15.Y AS Y FROM P0_6, a, P8_15 WHERE P0_6.Y = P8_15.X \n" +
        "AND P0_6.Y = a.X")

      p0_15.createOrReplaceTempView("P0_15")

      val res = sparkSession.sql("SELECT DISTINCT * FROM P0_15")
      val count = res.count

      println(s"res.count: $count")

      assert(12165 == count )
    }



    ignore("should run the following SPARK SQL2RDD Test") {

      //<P-0-1> (?X,?Y) :- <R> (?X, ?Y) .
      lazy val p0_1 = r

      //<P-1-2> (?X,?Y) :- <R> (?X, ?Y) .
      lazy val p1_2 = r

      //<P-2-3> (?X,?Y) :- <S> (?X, ?Y) .
      lazy val p2_3 = s

      //<P-3-4> (?X,?Y) :- <R> (?X, ?Y) .
      lazy val p3_4 = r

      //<P-4-5> (?X,?Y) :- <S> (?X, ?Y) .
      lazy val p4_5 = s

      //<P-6-7> (?X,?Y) :- <S> (?X, ?Y) .
      lazy val p6_7 = s

      //<P-7-8> (?X,?Y) :- <R> (?X, ?Y) .
      lazy val p7_8 = r

      //<P-8-9> (?X,?Y) :- <R> (?X, ?Y) .
      lazy val p8_9 = r

      //<P-9-10> (?X,?Y) :- <S> (?X, ?Y) .
      lazy val p9_10 = s

      //<P-10-11> (?X,?Y) :- <R> (?X, ?Y) .
      lazy val p10_11 = r

      //<P-9-11> (?X,?X) :- <A>(?X).
      //<P-9-11> (?X,?Y) :- <P-9-10> (?X, ?Z),<P-10-11> (?Z, ?Y).
      lazy val p9_11 = aMapped.union(myJoin(p9_10, p10_11)).persist(StorageLevel.MEMORY_AND_DISK)

      //<P-1-3> (?X,?X) :- <B>(?X).
      //<P-1-3> (?X,?Y) :- <P-1-2> (?X, ?Z),<P-2-3> (?Z, ?Y).
      lazy val p1_3 = bMapped.union(myJoin(p1_2, p2_3)).persist(StorageLevel.MEMORY_AND_DISK)

      //<P-0-3> (?X,?Y) :- <P-0-1> (?X, ?Z),<P-1-3> (?Z, ?Y).
      lazy val p0_3 = myJoin(p0_1, p1_3).persist(StorageLevel.MEMORY_AND_DISK)

      //<P-3-5> (?X,?Y) :- <P-1-3> (?X, ?Y) .
      lazy val p3_5 = p1_3

      //<P-5-7> (?X,?Y) :- <P-1-3> (?X, ?Y) .
      lazy val p5_7 = p1_3

      //<P-3-7> (?X,?Y) :- <P-3-5> (?X, ?Z),<P-5-7> (?Z, ?Y).
      //<P-3-7> (?X,?Y) :- <P-3-4>(?X, ?Z), <A>(?Z),<P-6-7> (?Z, ?Y).
      lazy val p3_7 = myJoin(p3_5, p5_7).union(myJoin(myJoin(p3_4, aMapped), p6_7)).persist(StorageLevel.MEMORY_AND_DISK)

      //<P-0-2> (?X,?Y) :- <P-0-1> (?X, ?Z),<P-1-2> (?Z, ?Y).
      lazy val p0_2 = myJoin(p0_1, p1_2).persist(StorageLevel.MEMORY_AND_DISK)

      //<P-4-6> (?X,?Y) :- <P-9-11> (?X, ?Y) .
      lazy val p4_6 = p9_11

      //<P-4-7> (?X,?Y) :- <A>(?X), <P-6-7>(?X, ?Y) .
      //<P-4-7> (?X,?Y) :- <P-4-5> (?X, ?Z),<P-5-7> (?Z, ?Y).
      lazy val p4_7 = myJoin(aMapped, p6_7).union( myJoin(p4_5, p5_7) ).persist(StorageLevel.MEMORY_AND_DISK)

      //<P-0-7> (?X,?Y) :- <P-0-3> (?X, ?Z),<P-3-7> (?Z, ?Y).
      //<P-0-7> (?X,?Y) :- <P-0-2>(?X, ?Z), <A>(?Z),<P-4-7> (?Z, ?Y).
      lazy val p0_7 = myJoin(p0_3, p3_7).union( myJoin(myJoin( p0_2, aMapped), p4_7) ).persist(StorageLevel.MEMORY_AND_DISK)

      //<P-7-9> (?X,?Y) :- <P-0-2> (?X, ?Y) .
      lazy val p7_9 = p0_2

      //<P-7-11> (?X,?Y) :- <P-7-9> (?X, ?Z),<P-9-11> (?Z, ?Y).
      //<P-7-11> (?X,?Y) :- <P-7-8>(?X, ?Z), <B>(?Z),<P-10-11> (?Z, ?Y).
      lazy val p7_11 = myJoin(p7_9, p9_11).union( myJoin( myJoin( p7_8, bMapped ), p10_11 )).persist(StorageLevel.MEMORY_AND_DISK)

      //<P-8-11> (?X,?Y) :- <P-8-9> (?X, ?Z),<P-9-11> (?Z, ?Y).
      //<P-8-11> (?X,?Y) :- <B>(?X),<P-10-11>(?X, ?Y).
      lazy val p8_11 =  myJoin( p8_9, p9_11).union( myJoin(bMapped, p10_11) ).persist(StorageLevel.MEMORY_AND_DISK)

      //<P-3-6> (?X,?Y) :- <P-8-11> (?X, ?Y) .
      lazy val p3_6 = p8_11

      //<P-13-15> (?X,?Y) :- <P-9-11> (?X, ?Y) .
      lazy val p13_15 = p9_11

      //<P-11-13> (?X,?Y) :- <P-1-3> (?X, ?Y) .
      lazy val p11_13 = p1_3

      //<P-11-15> (?X,?Y) :- <P-11-13> (?X, ?Z),<P-13-15> (?Z, ?Y).
      lazy val p11_15 =  myJoin(p11_13, p13_15).persist(StorageLevel.MEMORY_AND_DISK)

      //<P-7-15> (?X,?Y) :- <P-7-11> (?X, ?Z),<P-11-15> (?Z, ?Y).
      lazy val p7_15 = myJoin( p7_11, p11_15 ).persist(StorageLevel.MEMORY_AND_DISK)

      //<P-8-15> (?X,?Y) :- <P-8-11> (?X, ?Z),<P-11-15> (?Z, ?Y).
      lazy val p8_15 = myJoin(p8_11, p11_15).persist(StorageLevel.MEMORY_AND_DISK)

      //<P-0-6> (?X,?Y) :- <P-0-3> (?X, ?Z),<P-3-6> (?Z, ?Y).
      //<P-0-6> (?X,?Y) :- <P-0-2>(?X, ?Z), <A>(?Z),<P-4-6> (?Z, ?Y).
      lazy val p0_6 = myJoin(p0_3, p3_6).union(myJoin(myJoin(p0_2, aMapped), p4_6)).persist(StorageLevel.MEMORY_AND_DISK)

      //<P-0-15> (?X,?Y) :- <P-0-7> (?X, ?Z),<P-7-15> (?Z, ?Y).
      //<P-0-15> (?X,?Y) :- <P-0-6>(?X, ?Z), <A>(?Z),<P-8-15> (?Z, ?Y).
      lazy val p0_15 = myJoin(p0_7, p7_15).union(
              myJoin( myJoin(p0_6, aMapped), p8_15 )).persist(StorageLevel.MEMORY_AND_DISK)

      lazy val count =  p0_15.distinct.count
      println(s"p0_15.distinct.count: $count")

      assert( 12165 == count)
    }
  }
}
