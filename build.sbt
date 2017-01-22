name := "quotepersistence"

organization := "io.github.silvaren"

version := "1.0"

scalaVersion := "2.11.0"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

crossPaths := false

libraryDependencies += "io.github.silvaren" % "quoteparser" % "1.0"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "1.1.1"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.0"
libraryDependencies += "com.fatboyindustrial.gson-jodatime-serialisers" % "gson-jodatime-serialisers" % "1.4.0"
libraryDependencies += "net.lingala.zip4j" % "zip4j" % "1.3.2"

libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test->default"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
libraryDependencies += "de.flapdoodle.embed" % "de.flapdoodle.embed.mongo" % "1.50.3" % "test"
libraryDependencies += "org.mockito" % "mockito-core" % "2.6.4" % "test"


