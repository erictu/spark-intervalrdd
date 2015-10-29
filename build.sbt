name := "spark-intervalrdd"

version := "0.3-SNAPSHOT"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"

libraryDependencies += "org.bdgenomics.adam" %% "adam-cli" % "0.18.0"
libraryDependencies += "org.bdgenomics.adam" %% "adam-core" % "0.18.0"
libraryDependencies += "org.bdgenomics.adam" %% "adam-core" % "0.18.0" % "test" classifier "tests"
libraryDependencies += "org.bdgenomics.utils" %% "utils-misc" % "0.2.3" % "test" classifier "tests"

//spName := := "amplab/spark-intervalrdd"

//sparkVersion := "1.5.0"

//sparkComponents += "core"

publishMavenStyle := true

licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"

javaOptions in test += "-Xmx2G"

fork in test := true
