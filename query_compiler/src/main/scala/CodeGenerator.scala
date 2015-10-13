package DunceCap

/*
Standalone executable that Chris runs. Takes in GHD from JSON and
generates code.
*/

object CodeGenerator {
  def main(args:Array[String]) = {
    if(args.length == 1){
      val ghd = Generator.GHDFromJSON(args(0))
      Generator.run(ghd)
    } else{
      println("""Usage: "./target/start/ DunceCap.CodeGenerator <JSON GHD>" """)
    }
  }
}
