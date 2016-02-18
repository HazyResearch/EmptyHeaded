///////////////////////////////////////////////////////////////////////////////
// Parser Wrappers
//
// Each parser needs a wrapper class which is exposed/easily accessible 
// in Python.
///////////////////////////////////////////////////////////////////////////////
package duncecap

//Wrapper class to parse SQL
class SQL(val query:String) {
  def parse():IR ={
    println("Parsing SQL: " + query)
    new IR()
  }
}
//Wrapper class to parse Datalog
class Datalog(val query:String) {
  def parse():IR ={
    val ir = DatalogParser.run(query)
    println(ir)
    ir
  }
}