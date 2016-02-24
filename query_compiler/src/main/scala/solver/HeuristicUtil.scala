package duncecap

/**
 * Created by sctu on 2/23/16.
 */
object HeuristicUtil {
  def getGHDsOfMinHeight(candidates:List[GHD]) : List[GHD] = {
    val minHeight = candidates.map(_.depth).min
    println(s"""minHeight ${minHeight}""")
    candidates.filter(_.depth == minHeight)
  }
}
