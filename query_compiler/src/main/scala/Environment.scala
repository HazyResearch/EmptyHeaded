package DunceCap

import scala.collection.mutable
import java.nio.file.{Paths, Files}
import java.io.{FileWriter, File, BufferedWriter}
import scala.util.parsing.json._
import scala.io._

/*
  Stores information about what is in the database and 
  current configuration of the database. This is necessary
  for code generation information.
*/

object Environment {
  var config:Map[String,Any] = Map[String,Any]()
  def fromJSON(filename:String) = {
    val fileContents = Source.fromFile(filename +"/config.json").getLines.mkString
    val configIn:Map[String,Any] = JSON.parseFull(fileContents) match {
      case Some(map: Map[String, Any]) => map
      case _ => Map()
    }
    config = configIn
  }
}
