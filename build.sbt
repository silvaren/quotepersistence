name := "quotepersistence"

version := "1.0"

scalaVersion := "2.11.0"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

crossPaths := false

libraryDependencies += "io.github.silvaren" % "quoteparser" % "1.0"
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "1.1.1"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.0"
libraryDependencies += "com.fatboyindustrial.gson-jodatime-serialisers" % "gson-jodatime-serialisers" % "1.4.0"

libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test->default"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"




