name := "spark-intervalrdd"

version := "0.1-SNAPSHOT"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0"

publishMavenStyle := true
resolvers += Resolver.mavenLocal

licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test"
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.2" % "test"
libraryDependencies += "org.bdgenomics.utils" %% "utils-metrics" % "0.2.3"
libraryDependencies += "org.bdgenomics.adam" %% "adam-cli" % "0.18.3-SNAPSHOT"
libraryDependencies += "org.bdgenomics.adam" %% "adam-core" % "0.18.3-SNAPSHOT"
libraryDependencies += "org.bdgenomics.adam" %% "adam-core" % "0.18.3-SNAPSHOT" % "test" classifier "tests"
libraryDependencies += "org.bdgenomics.utils" %% "utils-misc" % "0.2.3" % "test" classifier "tests"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

javaOptions in test += "-Xmx1024m"
javaOptions in test += "-Xms1024m"
javaOptions in Test += "-agentpath:/path/to/yjp"

fork in test := true
