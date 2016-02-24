///////////////////////////////////////////////////////////////////////////////
// Generates the code that loads and creates the database.
//
///////////////////////////////////////////////////////////////////////////////
package duncecap

import scala.collection.mutable.ListBuffer
import sys.process._
import java.io.{FileWriter, File, BufferedWriter}
import scala.collection.mutable.Set
import java.nio.file.{Paths, Files}
import util.control.Breaks._

object QueryPlan{
  //loads the relations from disk (if nesc.)
  //and encodes them, then spills the encodings to disk
  //next builds the tries and spills to disk
  def build(ir:IR){
    println("creating DAG")
  }
}