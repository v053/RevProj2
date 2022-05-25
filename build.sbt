ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "HelloScala"
  )

// https://mvnrepository.com/artifact/org.vegas-viz/vegas
libraryDependencies += "org.vegas-viz" %% "vegas" % "0.3.11"