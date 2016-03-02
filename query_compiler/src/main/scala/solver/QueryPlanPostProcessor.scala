package duncecap

abstract trait QueryPlanPostProcessor {
  def doPostProcessingPass
  def getQueryPlan(prevRules:List[Rule]): List[Rule]
}