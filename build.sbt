// build.sbt --- Scala build tool settings

scalaVersion := "2.11.6"

scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-Ywarn-adapted-args", "-Ywarn-dead-code", "-Ywarn-numeric-widen", "-Ywarn-inaccessible")

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

scalacOptions in Test ++= Seq("-Yrangepos")

libraryDependencies ++= List(
  "org.scalactic" % "scalactic_2.11" % "2.2.1",
  "org.apache.spark" %% "spark-graphx" % "1.3.0",
  "org.specs2" %% "specs2-core" % "3.3.1" % "test",
  "org.specs2" %% "specs2-matcher-extra" % "3.3.1" % "test"
)

testOptions in Test += Tests.Argument("-oD")

scalariformSettings
