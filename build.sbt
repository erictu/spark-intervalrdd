name := "spark-intervalrdd"

version := "0.3"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.10.4"

spName := "amplab/spark-intervalrdd"

sparkVersion := "1.5.0"

sparkComponents += "core"

publishMavenStyle := true

licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"

// Run tests with more memory
javaOptions in test += "-Xmx2G"

fork in test := true
