package DunceCap

case class Encoding(val node:String)
case class Relation(val name:String, val storage:String)
case class Config(val layout:String,
                  val numNUMA:Int,
                  val algorithm:String,
                  val database:String,
                  val numThreads:Int,
                  val encodings:Encoding,
                  val relations:List[Relation],
                  val memory:String,
                  val schemas:List[Schema])
case class Attribute(val attrType:String, val encoding:String)
case class Schema(val name:String, val attributes:List[Attribute], val orderings:List[List[Int]], val annotation:String)