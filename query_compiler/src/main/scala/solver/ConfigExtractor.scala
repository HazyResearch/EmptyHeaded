package DunceCap

case class Relation(val name:String, val storage:String)
case class DatabaseConfig(val layout:String,
                  val numNUMA:Int,
                  val algorithm:String,
                  val database:String,
                  val numThreads:Int,
                  val encodings:Map[String, String],
                  val relations:Map[String, String],
                  val memory:String,
                  val schemas:Map[String, Schema])
case class Attribute(val attrType:String, val encoding:String)
case class Schema(val attributes:List[Attribute], val orderings:List[List[Int]], val annotation:String)