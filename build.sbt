import AssemblyKeys._ 
assemblySettings

/** Project */
name := "signal-collect-evaluation"

version := "2.1.0-SNAPSHOT"

organization := "com.signalcollect"

scalaVersion := "2.11.1"

scalacOptions ++= Seq("-optimize", "-Yinline-warnings", "-feature", "-deprecation", "-Xelide-below", "INFO" )

parallelExecution in Test := false

net.virtualvoid.sbt.graph.Plugin.graphSettings

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

EclipseKeys.withSource := true

jarName in assembly := "signal-collect-evaluation-2.1-SNAPSHOT.jar"

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  cp filter {_.data.getName == "minlog-1.2.jar"}
}

/** Dependencies */
libraryDependencies ++= Seq(
 "com.google.guava" % "guava" % "13.0.1" force(),
 "com.google.gdata" % "core" % "1.47.1",
 "org.scala-lang" % "scala-library" % "2.11.1"  % "compile",
 "commons-io" % "commons-io" % "2.4" force(),
 "commons-codec" % "commons-codec" % "1.7"  % "compile",
 "junit" % "junit" % "4.8.2"  % "test",
 "org.specs2" %% "specs2" % "2.3.11"  % "test"
  )
