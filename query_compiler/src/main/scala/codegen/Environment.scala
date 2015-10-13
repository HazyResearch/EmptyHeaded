package DunceCap

class Relation(
                val name:String,
                val types:List[String],
                val encodings:List[String],
                val annotationType:String = "void*"
                ){

  override def equals(that: Any): Boolean =
    that match {
      case that: Relation => that.encodings.equals(encodings) && that.types.equals(types) && that.name.equals(name)
      case _ => false
    }
}