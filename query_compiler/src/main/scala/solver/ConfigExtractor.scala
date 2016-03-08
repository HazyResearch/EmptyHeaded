package duncecap.serialized

case class Relation(val name:String, val storage:String)
case class DatabaseConfig(val layout:String,
                  val numNUMA:Int,
                  val algorithm:String,
                  val database:String,
                  val numThreads:Int,
                  val encodings:Map[String, String],
                  var relations:Map[String, String],
                  val memory:String,
                  var schemas:Map[String, Schema],
                  var resultName:String,
                  var resultOrdering:List[Int])
case class Attribute(val attrType:String, val encoding:String)
case class Schema(val attributes:List[Attribute], val orderings:List[List[Int]], val annotation:String)