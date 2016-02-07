package DunceCap

object HeuristicUtils {
  def getGHDsWithMinBags(candidates:List[GHD]): List[GHD] = {
    val minNumBags = candidates.map(_.numBags).min
    candidates.filter(c => {c.numBags == minNumBags})
  }

  def getGHDsWithMaxCoveringRoot(candidates:List[GHD]): List[GHD] = {
    val candidateAndCovering = candidates.map(c => {
      val numCovered = c.outputRelation.attrNames.filter(attr => {
        c.root.attrSet.contains(attr)
      }).size
      (c, numCovered)
    })
    val maxNumCovered = candidateAndCovering.unzip._2.max
    candidateAndCovering.filter(c => (c._2 == maxNumCovered)).unzip._1
  }

  def getGHDsWithSelectionsPushedDown(candidates:List[GHD]): List[GHD] = {
    val candidateAndSelectDepth = candidates.map(c => {
      val selectDepth = c.root.map(node => {
          node.level * node.getSelectedAttrs().size
      }).sum
      (c, selectDepth)
    })
    val maxSelectDepth = candidateAndSelectDepth.unzip._2.max
    candidateAndSelectDepth.filter(c => (c._2 == maxSelectDepth)).unzip._1
  }

  def getGHDsOfMinHeight(candidates:List[GHD]) : List[GHD] = {
    val minHeight = candidates.map(_.depth).min
    candidates.filter(_.depth == minHeight)
  }
}
