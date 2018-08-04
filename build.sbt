name := "stypes"

version := "1.0"

scalaVersion := "2.11.8"

val graalVersion = "1.2.0"

val sparkVersion = "2.2.0"

val flinkVersion = "1.4.0"

val playVersion = "2.6.8"

useGpg := false

publishArtifact in Test := false

pomIncludeRepository := { x => false }

resolvers += Resolver.sonatypeRepo("public")

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


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

licenses := Seq("Apache License" -> url("http://www.apache.org/licenses/LICENSE-2.0"))

homepage := Some(url("https://github.com/srapisarda/stypes"))

organization := "com.github.srapisarda"

scmInfo := Some(
  ScmInfo(
    url("https://github.com/srapisarda/stypes"),
    "scm:git@github.com:srapisarda/stypes.git"
  )

)

developers := List(
  Developer(
    id    = "srapis01",
    name  = "Salvatore Rapisarda",
    email = "srapis01@dcs.bbk.ac.uk",
    url   = url("https://www.dcs.bbk.ac.uk/~srapis01")
  )
)

publishMavenStyle := true


lazy val buildSettings =  Seq(
  organization := (organization in ThisBuild).value,
  // use the same value as in the build scope, so it can be overriden by stampVersion
  version := (version in ThisBuild).value
)

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}