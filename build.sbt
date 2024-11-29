name := "hightload2024"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"
val sparkVersion = "3.5.0"
val circeVersion = "0.14.6"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4" % "provided",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.12.600",
  "com.hierynomus" % "sshj" % "0.37.0",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion
)

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

lazy val root = (project in file("."))
  .in(file("."))
  .settings(
    assemblyJarName := s"${name.value}-${version.value}.jar",
    assembly / assemblyMergeStrategy := {
      case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeDependency(true),
    assembly / assemblyOption := (assembly / assemblyOption).value.withIncludeScala(true)

  )

assembly / artifact := {
  val art = (assembly / artifact).value
  art.withClassifier(Some("assembly"))
}

addArtifact(assembly / artifact, assembly)