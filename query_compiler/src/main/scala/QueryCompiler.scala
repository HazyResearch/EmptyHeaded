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
      val ghd = DCParser.run(ln)
      //Generator.run(ghd)
      print(">")
    } 
  }
  def main(args:Array[String]) = {
    println("ARGS LENGTH: " + args.length + " " + args(2) + " " + args(1))
    if( (args.length == 2) || (args.length == 3)){
      Environment.fromJSON(args(1))
      if(args.length == 3){
        val ghd = DCParser.run(readFile(args(2)))
        val a = CPPGenerator.run(ghd)
      } else {
        getInput()
      }
    } else{
      println("""Usage: "./target/start DunceCap.QueryCompiler <JSON config>" or "./target/start DunceCap.QueryCompiler <JSON config> <datalog file>" """)
    }
  }
}
