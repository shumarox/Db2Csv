name := "Db2Csv"
version := "0.0.1"

scalaVersion := "3.3.0"

crossPaths := false

scalacOptions ++= Seq("-encoding", "UTF-8")
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
)

autoScalaLibrary := true

libraryDependencies += "com.oracle.database.jdbc" % "ojdbc8" % "23.2.0.0"
