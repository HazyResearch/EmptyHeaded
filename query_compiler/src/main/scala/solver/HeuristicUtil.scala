package duncecap

/**
 * Created by sctu on 2/23/16.
 */
object HeuristicUtil {
  def getGHDsOfMinHeight(candidates:List[GHD]) : List[GHD] = {
    val minHeight = candidates.map(_.depth).min
    candidates.filter(_.depth == minHeight)
  }

  def getGHDsWithMaxCoveringRoot(candidates:List[GHD]): List[GHD] = {
    val candidateAndCovering = candidates.map(c => {
      val numCovered = c.outputRelation.attrs.values.filter(attr => {
        c.root.attrSet.contains(attr)
      }).size
      (c, numCovered)
    })
    val maxNumCovered = candidateAndCovering.unzip._2.max
    candidateAndCovering.filter(c => {c._2 == maxNumCovered}).unzip._1
  }

  def getGHDsWithMinBags(candidates:List[GHD]): List[GHD] = {
    val minNumBags = candidates.map(_.numBags).min
    candidates.filter(c => {c.numBags == minNumBags})
  }
}
