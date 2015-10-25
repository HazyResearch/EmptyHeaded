package DunceCap

import net.liftweb.json._

import scala.io._

import java.io.{FileOutputStream, PrintWriter, File}

import net.liftweb.json.Serialization.{read, write}
/*
  Stores information about what is in the database and 
  current configuration of the database. This is necessary
  for code generation information.
*/

case class RelationNotFoundException(what:String)  extends Exception

object Environment {
  var config:DatabaseConfig = null

  def fromJSON(filename:String) = {
    val fileContents = Source.fromFile(filename).getLines.mkString
    implicit val formats = DefaultFormats
    config = parse(fileContents).extract[DatabaseConfig]
  }
  def toJSON() = {
    val filename = config.database+"/config.json"
    implicit val formats = Serialization.formats(NoTypeHints)
    scala.tools.nsc.io.File(filename).writeAll(write(config))    
    
    /*
    val file = "tmp.json"
    val output = new PrintStream(new FileOutputStream(
        new File(file)))
    output.println(config)
    */
  }

  /**
   * Check that a relation with this name and this number of attributes has been loaded
   */
  def isLoaded(queryRelation: QueryRelation): Boolean = {
    config.schemas.get(queryRelation.name)
      .map(schema => schema.attributes.length == queryRelation.attrs.length).getOrElse(false)
  }

  /**
   * @param queryRelation find the query relation with this name and attrs and set it's annotation as in the config
   * @return boolean indicates whether this was successful (may not be if queryRelation does not exist)
   */
  def setAnnotationAccordingToConfig(queryRelation: QueryRelation): Boolean = {
   if (isLoaded(queryRelation)) {
     config.schemas.get(queryRelation.name)
       .filter(schema => schema.attributes.length == queryRelation.attrs.length)
       .map(schema => queryRelation.annotationType = schema.annotation)
      return true
    }
    return false
  }
}
