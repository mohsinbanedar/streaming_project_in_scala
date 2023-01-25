name := "ironsolver-poc"

version := "1.2"

resolvers ++= Seq(
  Resolver.typesafeRepo("releases"),
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Typesafe repository releases" at "https://repo.typesafe.com/typesafe/releases/",
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Twitter Repo" at "https://maven.twttr.com",
  "eaio.com" at "https://eaio.com/maven2"
)
libraryDependencies ++= Seq(

  "com.typesafe" % "config" % "1.3.3"

)

val sparkSql = "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
val json = "io.spray" %%  "spray-json" % "1.3.5"

lazy val tools = (project in file("spark/Tools"))
  .settings(
    name := "Tools",
    organization := "com.cacoveanu.spark.tools",
    libraryDependencies ++= Seq(
      sparkSql,
      json
    )
  )
//val sparkVersion = "3.1.2"
val sparkVersion = "3.3.1"
//val sparkVersion = "2.4.8"
//libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.10.1"
//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.10.1"
//libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.10.1"
libraryDependencies += "org.apache.spark"  % "spark-sql_2.12" % sparkVersion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.7.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.3.0"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.0.0-RC3"

//libraryDependencies += "com.github.seratch" %% "awscala" % "0.9.+"
libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.2" % "1.0.0"
libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.11.0"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.3.0"
//libraryDependencies += "org.scala-lang.modules" %% "scala-jdk-collection-converters" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"

libraryDependencies += "joda-time" % "joda-time" % "2.10.7"

//dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
//dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
//x§dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.8.7"
libraryDependencies ++= Seq(
  "com.typesafe"      % "config"                          % "1.4.1",
 // "ch.qos.logback"    % "logback-classic"                 % "1.2.3",
  "org.apache.spark"  % "spark-streaming_2.12"            % sparkVersion,
  "org.apache.spark"  % "spark-sql_2.12"            % sparkVersion,
  "org.apache.spark"  % "spark-core_2.12"            % sparkVersion,
  "org.apache.spark"  % "spark-avro_2.12" % sparkVersion,
  "org.apache.spark"  % "spark-streaming-kafka-0-10_2.12" % sparkVersion,
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,
  "org.scalactic" %% "scalactic" % "3.2.9",
  "org.scalatest" %% "scalatest" % "3.2.9" % "test",




)

/*val myAssemblySettings = Seq(
  ThisBuild / assemblyMergeStrategy := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case _ => MergeStrategy.last
  }
)

lazy val commonSettings = Seq(
  organization := "com.ironsource",
  scalaVersion := "2.12.15",
  test in assembly := {},
  fork in run := true
)

lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings(myAssemblySettings: _*).
  settings(
    mainClass in assembly := Some("com.ironsource")
  )

artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

def assembly = ???

addArtifact(artifact in (Compile, assembly), assembly)

*/
