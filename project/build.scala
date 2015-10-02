import sbt._
object intervalrdd extends Build
{
  lazy val root =
    Project("root", file(".")) dependsOn(unfiltered)
  lazy val unfiltered =
    RootProject(uri("../interval-tree"))
}