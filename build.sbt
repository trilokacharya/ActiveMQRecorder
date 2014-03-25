name := "StreamingWriter"

version := "1.0"

scalaVersion := "2.10.2"

//resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

resolvers += "SonaType" at "https://oss.sonatype.org/content/groups/public"

libraryDependencies ++= Seq(
  //  "org.scalacheck" %% "scalacheck" % "1.10.1" % "test",
  //  "org.scala-lang.modules" %% "scala-async" % "0.9.0-M2",
  //"org.apache.activemq" % "activemq-all" % "5.0.0",
  "org.apache.activemq" % "activemq-all" % "5.6.0",
    "com.jsuereth" % "scala-arm_2.10" % "1.3",
  "com.netflix.rxjava" % "rxjava-scala" % "0.15.1")