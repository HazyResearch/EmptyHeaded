import com.typesafe.sbt.SbtStartScript
seq(SbtStartScript.startScriptForClassesSettings: _*)

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

name := "query_compiler"

//resolvers += "Sonatype (releases)" at "https://oss.sonatype.org/content/repositories/releases/"
scalaVersion := "2.11.2"

mainClass := Some("DunceCap.QueryCompiler")
publishTo := Some(Resolver.file("file",  new File( "exec" )) )

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % "2.11.2",
  "org.scala-lang" % "jline" % "2.10.2",
  "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
  "org.apache.commons" % "commons-lang3" % "3.1",
  "io.argonaut" %% "argonaut" % "6.0.4",
  "org.apache.commons" % "commons-math3" % "3.2",
  "org.zeromq" % "jeromq" % "0.3.1",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.2"
)

