import AssemblyKeys._ 
assemblySettings

/** Project */
name := "signal-collect-evaluation"

version := "2.1.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.10.1"

EclipseKeys.withSource := true

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter {_.data.getName == "minlog-1.2.jar"}
}

/** Dependencies */
libraryDependencies ++= Seq(
 "org.scala-lang" % "scala-library" % "2.10.1"  % "compile",
 "com.google.collections" % "google-collections" % "1.0" ,
 "ch.ethz.ganymed" % "ganymed-ssh2" % "build210"  % "compile",
 "commons-io" % "commons-io" % "2.4" ,
 "commons-codec" % "commons-codec" % "1.7"  % "compile",
 "junit" % "junit" % "4.8.2"  % "test",
 "org.specs2" % "specs2_2.10" % "1.13"  % "test"
  )
