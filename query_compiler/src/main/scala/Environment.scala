package DunceCap

import net.liftweb.json._

import scala.io._

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

  /**
   * Check that a relation with this name and this number of attributes has been loaded
   */
  def isLoaded(queryRelation: QueryRelation): Boolean = {
    config.schemas.find(schema => schema.name == queryRelation.name
      && schema.attributes.length == queryRelation.attrs.length).isDefined
  }

  /**
   * @param queryRelation find the query relation with this name and attrs and set it's annotation as in the config
   * @return boolean indicates whether this was successful (may not be is queryRelation does not exist)
   */
  def setAnnotationAccordingToConfig(queryRelation: QueryRelation): Boolean = {
    if (isLoaded(queryRelation)) {
      config.schemas.find(schema => schema.name == queryRelation.name
        && schema.attributes.length == queryRelation.attrs.length).map(schema => {
        queryRelation.annotationType = schema.annotation
      })
      return true
    }
    return false
  }
}
