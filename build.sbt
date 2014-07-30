import AssemblyKeys._ 
assemblySettings

/** Project */
name := "signal-collect-evaluation"

version := "2.1.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.11.2"

scalacOptions ++= Seq("-optimize", "-Yinline-warnings", "-feature", "-deprecation", "-Xelide-below", "INFO" )

parallelExecution in Test := false

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true

jarName in assembly := "signal-collect-evaluation-2.1-SNAPSHOT.jar"

/** Dependencies */
libraryDependencies ++= Seq(
 "org.scala-lang" % "scala-library" % "2.11.2"  % "compile",
 "com.google.collections" % "google-collections" % "1.0" ,
 "commons-io" % "commons-io" % "2.4",
 "commons-codec" % "commons-codec" % "1.7"  % "compile"
  )
