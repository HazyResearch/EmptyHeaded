package DunceCap

abstract trait QueryPlanPostProcessor {
  def doPostProcessingPass
  def getQueryPlan: QueryPlan
}