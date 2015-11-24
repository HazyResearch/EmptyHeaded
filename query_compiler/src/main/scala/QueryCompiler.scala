package DunceCap

import java.io.{FileOutputStream, PrintStream, File}

case class Config(directory: Option[String] = None,
                  dbConfig:String = "",
                  nprrOnly:Boolean = false,
                  bagDedup:Boolean = true,
                  codeGen:Option[String] = None,
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
      opt[String]('g', "codegen") action { (x, c) =>
        c.copy(codeGen = Some(x))} text("File to JSON query plan, runs only code generation with this argument.")
      opt[String]('c', "db-config") required() valueName("<file>") action { (x, c) =>
        c.copy(dbConfig = x)} text("database config file")
      opt[Unit]('n', "nprr-only") action { (_, c) =>
        c.copy(nprrOnly = true)} text("whether to use only NPRR (single bag GHD), defaults to false")
      opt[Unit]("no-bag-dedup") action { (_, c) =>
        c.copy(bagDedup = false)} text("whether to deduplicate bags in the GHD")
      opt[Unit]('f', "read-query-from-file") action { (_, c) =>
        c.copy(readQueryFromFile = true)} text("whether to read the query from a file, defaults to false")
      arg[String]("<query>") action { (x, c) =>
        c.copy(query = x)} text("query")
      help("help") text("prints this usage text")
    }

    parser.parse(args, Config()) map { config =>
      Environment.fromJSON(config.dbConfig)
      val queryString =
        if (config.readQueryFromFile) {
          readFile(config.query)
        } else {
          config.query
        }
        

      val queryPlans:QueryPlans = config.codeGen match {
        case None => QueryPlans(List[QueryPlan](DCParser.run(queryString, config))) //SUSAN FIXME when we get code chunks we get a list of query plans for code generation
        case Some(g) => QP.fromJSON(g)
      }

      val output = config.directory match {
        case Some(d) => {
          new PrintStream(new FileOutputStream(
            new File(d + "/q"+queryString.hashCode+".json"),false))
        } 
        case _ => { 
          System.out
        }
      }

      if (!config.explain) {
        CPPGenerator.run(queryPlans)
      } else {
        output.print(queryPlans)
      }
      output.close
    } getOrElse {
      // arguments are bad, usage message will have been displayed
    }
  }
}
