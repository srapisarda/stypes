name := "stypes"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq( "fr.lirmm.graphik" % "graal-core" % "1.2.0"
                ,"fr.lirmm.graphik" % "graal-forward-chaining" % "1.2.0"
                ,"fr.lirmm.graphik" % "graal-backward-chaining" % "1.2.0"
                ,"fr.lirmm.graphik" % "graal-io-dlgp" % "1.2.0"
                ,"fr.lirmm.graphik" % "graal-store-rdbms" % "1.2.0"
                ,"fr.lirmm.graphik" % "graal-homomorphism" % "1.2.0"
                ,"junit" % "junit" % "4.10" % "test"
                ,"org.scalatest" %% "scalatest" % "3.0.4"
                ,"ch.qos.logback" % "logback-classic" % "1.2.3"
                ,"com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0"
                ,"org.apache.spark" %% "spark-core" % "2.2.0"
                ,"org.apache.spark" %% "spark-sql" % "2.2.0"
            )