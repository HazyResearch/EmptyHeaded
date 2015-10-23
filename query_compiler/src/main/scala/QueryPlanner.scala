package DunceCap

/*
  Susans file. Standalone executable runs from parsing through GHD gen.
  Should spill JSON GHD to disk.
*/

object QueryPlanner {
  def readFile(file:String): String = {
    val source = scala.io.Source.fromFile(file)
    val line = try source.mkString finally source.close()
    line
  }
  def getInput() : Unit = {
    print(">")
    for (ln <- io.Source.stdin.getLines){
      if(ln == "exit()"){
        return
      } 
      DCParser.run(ln)
      print(">")
    } 
  }
  def main(args:Array[String]) = {
    if( (args.length == 1) || (args.length == 2)){
      Environment.fromJSON(args(0))
      if(args.length == 2){
        DCParser.run(args(1))
      } else {
        getInput()
      }
    } else{
      println("""Usage: "./target/start/QueryCompiler <JSON config>" or "./target/start/QueryCompiler <JSON config> <datalog query>" """)
    }
  }
}
