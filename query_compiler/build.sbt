resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

name := "query_compiler"

//resolvers += "Sonatype (releases)" at "https://oss.sonatype.org/content/repositories/releases/"
scalaVersion := "2.11.7"

publishTo := Some(Resolver.file("file",  new File( "exec" )) )

packAutoSettings

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % "2.11.2",
  "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
  "org.apache.commons" % "commons-lang3" % "3.1",
  "org.apache.commons" % "commons-math3" % "3.2",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.2",
  "net.liftweb" %% "lift-json" % "2.6+",
  "com.github.scopt" %% "scopt" % "3.2.0",
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.apache.spark" %% "spark-graphx" % "1.6.1"
)
