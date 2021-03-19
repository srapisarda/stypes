name := "stypes"

version := "1.1.0"

scalaVersion := "2.12.6"

crossScalaVersions := Seq("2.11.8", "2.12.6")

val graalVersion = "1.2.0"

//useGpg := false

publishArtifact in Test := false

pomIncludeRepository := { x => false }

resolvers += Resolver.sonatypeRepo("public")


libraryDependencies ++= Seq( "fr.lirmm.graphik" % "graal-core" % graalVersion
                ,"fr.lirmm.graphik" % "graal-forward-chaining" % graalVersion
                ,"fr.lirmm.graphik" % "graal-backward-chaining" % graalVersion
                ,"fr.lirmm.graphik" % "graal-io-dlgp" % graalVersion
                ,"fr.lirmm.graphik" % "graal-store-rdbms" % graalVersion
                ,"fr.lirmm.graphik" % "graal-homomorphism" % graalVersion
                ,"com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
                ,"com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0"
                ,"com.github.jsqlparser" % "jsqlparser" % "0.9"
                // test
                ,"org.scalatest" %% "scalatest" % "3.0.4" % "test"
                ,"ch.qos.logback" % "logback-classic" % "1.2.3" % "test"
                ,"junit" % "junit" % "4.10" % "test"
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