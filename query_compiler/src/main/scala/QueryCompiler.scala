///////////////////////////////////////////////////////////////////////////////
// Wrapper classes for the query compiler.
// The query compiler needs to know the schema of every relation in the db.
///////////////////////////////////////////////////////////////////////////////
package duncecap

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.io.FileInputStream
import java.io.ObjectInputStream
import sys.process._

//Defines schema types for relations
case class Schema(
  val externalAttributeTypes:List[String], 
  val externalAnnotationTypes:List[String]
  ) extends Serializable {
  val attributeTypes:List[String] = checkAttributes()
  val annotationTypes:List[String] = checkAnnotations()
  def getAttributeTypes:Array[String] = {externalAttributeTypes.toArray}
  def getAnnotationTypes:Array[String] = {externalAnnotationTypes.toArray}
  
  //Checks that the schema has valid types.
  private def checkAttributes():List[String] = {
    externalAttributeTypes.map(a => {
      if(!QueryCompiler.validAttributeTypes.contains(a))
        throw new Exception("Attribute type " + a + " in schema is not valid.")
      QueryCompiler.validAnnotationTypes(a)
    })
  }
  private def checkAnnotations():List[String] = {
    externalAnnotationTypes.map(a => {
      if(!QueryCompiler.validAnnotationTypes.contains(a))
        throw new Exception("Attribute type " + a + " in schema is not valid.")
      QueryCompiler.validAnnotationTypes(a)
    })
  }
}

//Defines relations
case class Relation(
  val name:String,  
  val schema:Schema,
  val filename:String,
  val df:Boolean
  ) extends Serializable {
  def getName():String = {name}
  def getSchema():Schema = {schema}
  def getFilename():String = {filename}
  def getDF():Boolean = {df}
}

//Configuration for the db
case class Config(
  val system:String, 
  val numThreads:Int, 
  val numSockets:Int,
  val layout:String,
  val memory:String
  ) extends Serializable {
  def getSystem():String = {system}
  def getNumThreads():Int = {numThreads}
  def getNumSockets():Int = {numSockets}
  def getLayout():String = {layout}
  def getMemory():String = {memory}
}

//Creates an instance of database (needed to compile queries)
case class DBInstance(val folder:String, val config:Config) extends Serializable {
  val relations:ListBuffer[Relation] = ListBuffer[Relation]()
  //map from relation name to index in listbuffer
  val name2relation:Map[String,Int] = Map[String,Int]()
  def addRelation(r:Relation){
    name2relation += ((r.name,relations.length))
    relations += r
  }
  def getFolder():String = {folder}
  def getConfig():Config = {config}
  def getNumRelations():Int = {relations.length}
  //kind of a hack but we make it look like a list
  def getRelation(i:Int):Relation = {relations(i)}
}

//Main class which compilation runs out of
class QueryCompiler(val db:DBInstance,val hash:String) extends Serializable{
  def getDBInstance():DBInstance = {db}

  def createDB(){
    CreateDB.loadAndEncode(db,hash)
  }

  //Parse a datalog statement and code generate it.
  def genTrieWrapper(rel:String) {
    Trie.run(db,db.relations(db.name2relation(rel)))
  }

  //Parse a datalog statement and code generate it.
  def datalog(query:String):String = {
    val ir = DatalogParser.run(query)
    println(ir)
    "Query.cpp"
  }

  //Parse a SQL statement and code generate it.
  def sql(query:String):String = {
    println("PARSING SQL")
    println(query)
    //run SQL parser
    //return IR
    "Query.cpp"
  }

  //run the GHD optimizer over an IR
  def optimize(ir:IR):IR = {
    println("running GHD optimizer")
    println(ir)
    ir
  }

  //code generate from an IR
  //return name of the cpp file
  def generate(ir:IR):String = {
    println("Code generate")
    "Query.cpp"
  }

  //saves the schema on disk
  def toDisk(){
    val result = s"""mkdir ${db.folder}""" !

    if(result == 1){
      throw new Exception("ERROR DATABASE FOLDER EXISTS OR IS AN INVALID PATH.")
    }

    val result2 = s"""mkdir ${db.folder}/libs""" !

    val fos = new FileOutputStream(db.folder+"/schema.bin")
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(this)
    oos.close()
  }
}

object QueryCompiler {
  //acceptable attribute types
  val validAttributeTypes = Map(
    //"Boolean" -> ,
    //"Byte",
    "uint32" -> "uint32_t",
    "int32" -> "int32_t",
    "int64" -> "uint64_t",
    "uint64" -> "int64_t"
    //"String",
    //"Float",
    //"Double",
    //"Date"
    )

  //All numeric types
  val validAnnotationTypes = Map(
    "uint32" -> "uint32_t",
    "int32" -> "int32_t",
    "int64" -> "int64_t",
    "uint64" -> "uint64_t",
    "float32" -> "float",
    "float64" -> "double"
  )

  //Special builder to conver arrays to lists 
  def buildSchema(attrTypes:Array[String],annoTypes:Array[String]) : Schema = {
    Schema(attrTypes.toList,annoTypes.toList)
  }
  //Build from a schema saved on disk
  def fromDisk(filename:String) : QueryCompiler = {
    val fos = new FileInputStream(filename)
    val oos = new ObjectInputStream(fos)
    val myqc = oos.readObject().asInstanceOf[QueryCompiler]
    oos.close()
    myqc
  }
}