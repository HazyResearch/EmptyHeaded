package DunceCap

import DunceCap.attr.Attr

object QueryRelationFactory {
  def createQueryRelationWithNoSelects(attrs:List[Attr]): QueryRelation = {
    val attrInfo = attrs.map(attr => {
      (attr, "", "")
    })
    new QueryRelation("", attrInfo)
  }
}