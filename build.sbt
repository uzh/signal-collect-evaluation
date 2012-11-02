import AssemblyKeys._ 
assemblySettings

/** Project */
name := "signal-collect-evaluation"

version := "2.0.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.10.0-RC1"

resolvers += "Typesafe Snapshot Repository" at "http://repo.typesafe.com/typesafe/snapshots/"

EclipseKeys.withSource := true

/** Dependencies */
libraryDependencies ++= Seq(
 "org.scala-lang" % "scala-library" % "2.10.0-RC1"  % "compile",
 "com.google.collections" % "google-collections" % "1.0" ,
 "ch.ethz.ganymed" % "ganymed-ssh2" % "build210"  % "compile",
 "commons-io" % "commons-io" % "2.4" ,
 "commons-codec" % "commons-codec" % "1.7"  % "compile",
 "junit" % "junit" % "4.8.2"  % "test",
 "org.specs2" % "specs2_2.10.0-RC1" % "1.12.2"  % "test"
  )