name := "StreamingWriter"

version := "1.0"

scalaVersion := "2.10.2"

resolvers += "SonaType" at "https://oss.sonatype.org/content/groups/public"

libraryDependencies ++= Seq(
  "org.apache.activemq" % "activemq-all" % "5.6.0",
    "com.jsuereth" % "scala-arm_2.10" % "1.3",
  "com.netflix.rxjava" % "rxjava-scala" % "0.15.1")
