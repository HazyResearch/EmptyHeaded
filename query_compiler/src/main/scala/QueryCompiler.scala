package DunceCap

import java.io.{FileOutputStream, PrintStream, File}

/*
  Susans file. Standalone executable runs from parsing through GHD gen.
  Should spill JSON GHD to disk.
*/

case class Config(directory: Option[String] = None,
                  dbConfig:String = "",
                  useGHD:Boolean = true,
                  readQueryFromFile:Boolean = false,
                  query:String = "",
                  explain:Boolean = false)

object QueryCompiler {
  def readFile(file:String): String = {
    val source = scala.io.Source.fromFile(file)
    val line = try source.mkString finally source.close()
    line
  }
  def main(args:Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("target/start") {
      head("Emptyheaded", "0.1")
      opt[Unit]("explain") action { (_, c) =>
        c.copy(explain = true) } text("show the query plan instead of running the query")
      opt[String]('d', "directory") action { (x, c) =>
        c.copy(directory = Some(x))} text("directory within EMPTYHEADED_HOME to write json output, by default prints to stdout when running just planner")
      opt[String]('c', "db-config") required() valueName("<file>") action { (x, c) =>
        c.copy(dbConfig = x)} text("database config file")
      opt[Boolean]('g', "use-ghd") action { (x, c) =>
        c.copy(useGHD = x)} text("whether to use GHD, defaults to true (TODO)")
      opt[Unit]('f', "read-query-from-file") action { (_, c) =>
        c.copy(readQueryFromFile = true)} text("whether to read the query from a file, defaults to false")
      arg[String]("<query>") action { (x, c) =>
        c.copy(query = x)} text("query")
    }

    // parser.parse returns Option[C]
    parser.parse(args, Config()) map { config =>
      Environment.fromJSON(config.dbConfig)
      val queryString =
        if (config.readQueryFromFile) {
          readFile(config.query)
        } else {
          config.query
        }
      val output = config.directory.map(dir => {
        Some(new PrintStream(new FileOutputStream(
          new File(new File(sys.env("EMPTYHEADED_HOME"), dir).getPath(), queryString.hashCode + ".json"))))
      }).getOrElse(
          if (config.explain) {
            Some(System.out)
          } else {
            None
          }
        )
      val queryPlan = DCParser.run(queryString)
      if (!config.explain) {
        CPPGenerator.run(queryPlan)
      }
      output.map(o => o.print(queryPlan))
      output.map(_.close)
    } getOrElse {
      // arguments are bad, usage message will have been displayed
    }
  }
}
