package duncecap

object IROptimizer {
  def applySubstitutionsToRule(rule:Rule, substitutions:Map[String, String]): Rule = {
    Rule(rule.result,
         rule.recursion,
         rule.operation,
         rule.order,
         rule.project,
         Join(rule.join.rels.map(rel => {
           if (substitutions.contains(rel.name)) {
             Rel(substitutions(rel.name), rel.attrs, rel.anno)
           } else {
             rel
           }
         })),
         rule.aggregations,
         rule.filters)
  }

  def dedupComputationsHelper(rules:List[Rule],
                                seen:List[Rule],
                                substitutions:Map[String, String]): List[Rule] = {
    val cur = applySubstitutionsToRule(rules.head, substitutions)
    val seenRule = seen.find(rule => cur.attrNameAgnosticEquals(rule))
    if (seenRule.isEmpty && !rules.tail.isEmpty) {
      cur::dedupComputationsHelper(rules.tail, cur::seen, substitutions)
    } else if  (seenRule.isEmpty && seenRule.isEmpty) {
      List(cur)
    } else if (rules.tail.isEmpty) {
      List()
    } else {
      dedupComputationsHelper(
        rules.tail,
        seen,
        substitutions + (cur.result.rel.name -> seenRule.get.result.rel.name))
    }
  }
  
  def dedupComputations(ir:IR): IR = {
    IR(dedupComputationsHelper(ir.rules, List(), Map()))
  }
}
