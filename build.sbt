name := "StreamingWriter"

version := "1.0"

scalaVersion := "2.10.3"

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
"org.scalaz" %% "scalaz-core" % "7.0.5",
"org.scalaz.stream" %% "scalaz-stream" % "0.3.1",
"org.scalacheck" %% "scalacheck" % "1.10.1" % "test",
"org.scala-lang.modules" %% "scala-async" % "0.9.0-M2",
"org.apache.activemq" % "activemq-core" % "5.0.0",
"com.netflix.rxjava" % "rxjava-scala" % "0.15.1")