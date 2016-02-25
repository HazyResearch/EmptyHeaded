package duncecap

abstract trait QueryPlanPostProcessor {
  def doPostProcessingPass
  def getQueryPlan: List[Rule]
}