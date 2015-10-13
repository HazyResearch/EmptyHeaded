package DunceCap

/*
  The whole stack runs in this file. Parse datalog
  through producing a GHD which passes a JSON object
  to the codegen.
*/

object QueryCompiler {
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
        DCParser.run(readFile(args(1)))
      } else {
        getInput()
      }
    } else{
      println("""Usage: "./target/start/ DunceCap.QueryCompiler <JSON config>" or "./target/start DunceCap.QueryCompiler <JSON config> <datalog file>" """)
    }
  }
}
