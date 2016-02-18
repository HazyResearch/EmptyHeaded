package duncecap

import duncecap.attr.{Attr, AttrType, AnnotationType}
import net.liftweb.json._

import scala.collection.mutable
import scala.io._

import net.liftweb.json.Serialization.{read, write, writePretty}
import scala.collection.mutable.Stack

/*
  Stores information about what is in the database and 
  current configuration of the database. This is necessary
  for code generation information.
*/

case class RelationNotFoundException(what:String)  extends Exception("Relation not found: " + what)

object Environment {
  var config:DatabaseConfig = null
  /*TODO: (sctu) for now we never update config's schemas */
  /*TODO: (sctu) for now we only check name and length of schema */
  var schemaStack:Stack[mutable.Map[String, Schema]] = Stack[mutable.Map[String, Schema]]()

  def fromJSON(filename:String) = {
    val fileContents = Source.fromFile(filename).getLines.mkString
    implicit val formats = DefaultFormats
    config = parse(fileContents).extract[DatabaseConfig]
    schemaStack.push(mutable.Map() ++ config.schemas)
  }

  def fromJsonString(jsonString:String) = {
    implicit val formats = DefaultFormats
    config = parse(jsonString).extract[DatabaseConfig]
    schemaStack.push(mutable.Map() ++ config.schemas)
  }

  def toJSON() = {
    val filename = config.database+"/config.json"
    implicit val formats = Serialization.formats(NoTypeHints)
    scala.tools.nsc.io.File(filename).writeAll(writePretty(config))
  }

  /**
   * @param queryRelation find the query relation with this name and attrs and set it's annotation as in the config
   * @return boolean indicates whether this was successful (may not be if queryRelation does not exist)
   */
  def setAnnotationAccordingToConfig(queryRelation: QueryRelation): Boolean = {
    val schema = schemaStack.find(schema => schema.get(queryRelation.name)
      .find(schema => schema.attributes.length == queryRelation.attrs.length).isDefined)
    schema.map(schema => queryRelation.annotationType = schema.get(queryRelation.name).get.annotation)
    return schema.isDefined
  }

  def getType(relName:String, attrPos:Int): AttrType = {
    val schemaOpt = schemaStack.find(schema => schema.get(relName)
      .find(schema => schema.attributes.length > attrPos).isDefined)
    val typeOpt = schemaOpt.map(schema => (schema.get(relName).get).attributes(attrPos).attrType)
    if(typeOpt.isDefined) {
      return typeOpt.get
    } else {
      throw NoTypeFoundException(relName, attrPos)
    }
  }

  def startScope(): Unit = {
    schemaStack.push(mutable.Map[String, Schema]())
  }

  def addRelation(rel:QueryRelation, outputType:List[AttrType]) = {
    // TODO: do this properly
    schemaStack.top += (rel.name -> Schema(outputType.map(attrType => Attribute(attrType, "")), List(List()), rel.annotationType))
  }

  def endScope(): Unit = {
    schemaStack.pop()
  }
}