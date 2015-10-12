package DunceCap

object QueryFileReader {
  def readFile(file:String): String = {
    val source = scala.io.Source.fromFile(file)
    val line = try source.mkString finally source.close()
    line
  }
}

object QueryCompiler extends App {
  val usage = "Usage: ./QueryCompiler <query file>"
  if(args.length == 1){ //reading a query from a file
    DCParser.run(QueryFileReader.readFile(args(0)))
  } else {
    println(usage)
    System.exit(1)
  }
}
