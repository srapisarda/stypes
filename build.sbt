name := "stypes"

version := "1.0"

scalaVersion := "2.11.8"

val graalVersion = "1.2.0"

val sparkVersion = "2.2.0"

val flinkVersion = "1.4.0"

libraryDependencies ++= Seq( "fr.lirmm.graphik" % "graal-core" % graalVersion
                ,"fr.lirmm.graphik" % "graal-forward-chaining" % graalVersion
                ,"fr.lirmm.graphik" % "graal-backward-chaining" % graalVersion
                ,"fr.lirmm.graphik" % "graal-io-dlgp" % graalVersion
                ,"fr.lirmm.graphik" % "graal-store-rdbms" % graalVersion
                ,"fr.lirmm.graphik" % "graal-homomorphism" % graalVersion
                ,"junit" % "junit" % "4.10" % "test"
                ,"org.scalatest" %% "scalatest" % "3.0.4"
                ,"ch.qos.logback" % "logback-classic" % "1.2.3"
                ,"com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0"
                ,"org.apache.spark" %% "spark-core" % sparkVersion
                ,"org.apache.spark" %% "spark-sql" % sparkVersion
                ,"org.apache.flink" %% "flink-streaming-scala" % flinkVersion
            )