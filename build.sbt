name := "StreamingWriter"

version := "1.0"

scalaVersion := "2.10.2"

resolvers += "SonaType" at "https://oss.sonatype.org/content/groups/public"

libraryDependencies ++= Seq(
 "org.apache.activemq" % "activemq-all" % "5.6.0",
 "com.jsuereth" % "scala-arm_2.10" % "1.3",
 "com.netflix.rxjava" % "rxjava-scala" % "0.15.1",
  "org.json4s" %% "json4s-native" % "3.2.7",
 "com.github.nscala-time" %% "nscala-time" % "0.8.0",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "junit" % "junit" % "4.10" % "test"
)
