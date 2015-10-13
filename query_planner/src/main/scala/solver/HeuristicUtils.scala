package DunceCap

object HeuristicUtils {
  def getGHDsWithMinBags(candidates:List[GHD]): List[GHD] = {
    val minNumBags = candidates.foldLeft(Int.MaxValue)((acc, c) => {
      if (c.numBags < acc)
        c.numBags
      else
        acc
    })
    candidates.filter(c => {c.numBags == minNumBags})
  }

  def getGHDsWithMaxCoveringRoot(candidates:List[GHD]): List[GHD] = {
    val candidateAndCovering = candidates.map(c => {
      val numCovered = c.outputRelation.attrNames.filter(attr => {
        c.root.attrSet.contains(attr)
      }).size
      (c, numCovered)
    })
    val maxNumCovered = candidateAndCovering.foldLeft(0)((acc, c) => {
      if (c._2 > acc)
        c._2
      else
        acc
    })
    candidateAndCovering.filter(c => {c._2 == maxNumCovered}).unzip._1
  }
}
