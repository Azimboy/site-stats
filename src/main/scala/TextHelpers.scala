import info.debatty.java.stringsimilarity.{JaroWinkler => JavaJaroWinkler, Levenshtein => JavaLevenshtein}

import scala.annotation.tailrec

object TextHelpers {

  @SerialVersionUID(1L)
  class JaroWinkler extends Serializable {
    private lazy val jaroWinkler = new JavaJaroWinkler()

    def distance(s1: String, s2: String): Double = jaroWinkler.similarity(s1, s2)
  }

  @SerialVersionUID(1L)
  class Levenshtein extends Serializable {
    private lazy val levenshtein = new JavaLevenshtein()

    def distance(s1: String, s2: String): Double = levenshtein.similarity(s1, s2)
  }

  val jaroWinkler = new JaroWinkler()

  case class TextData(text: String, similarity: Double)

  def findMostCommonAndSimilarText(texts: List[String]): String = {
    texts.map { i =>
      TextData(i, texts.map { j =>
        TextData(i, jaroWinkler.distance(i, j))
      }.map(_.similarity).sum)
    }.maxBy(_.similarity).text
  }

}
