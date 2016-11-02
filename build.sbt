name := "ticks2candles"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.10"
libraryDependencies += "ch.qos.logback" %  "logback-classic" % "1.1.7"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.5"

packAutoSettings

packResourceDir += (baseDirectory.value / "conf" -> "bin")