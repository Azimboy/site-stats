import info.debatty.java.stringsimilarity.{JaroWinkler => JavaJaroWinkler, Levenshtein => JavaLevenshtein}

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

  case class TextData(text: String, similarity: Double)

  val jaroWinkler = new JaroWinkler()

  def findMostCommonAndSimilarText(texts: List[String]): String = {
    texts.map { i =>
      TextData(i, texts.map { j =>
        TextData(i, jaroWinkler.distance(i, j))
      }.map(_.similarity).sum)
    }.maxBy(_.similarity).text
  }


  def getAddressScore(addressText: String): Option[Int] = {
    val streetPattern = """(\b\d+\s[A-Za-z\s]+)""".r
    val suitePattern = """\bSuite\s([A-Za-z\s]+)""".r
    val cityPattern = """\b([A-Za-z\s]+)\s?:""".r
    val statePostalPattern = """\b([A-Za-z\s]+)\s?(\w{1,2}\s?\d\w\s?\d\w)""".r
    val countryPattern = """\b([A-Za-z\s]+)$""".r

    val street = streetPattern.findFirstIn(addressText).map(_.trim)
    val suite = suitePattern.findFirstIn(addressText).map(_.trim)
    val city = cityPattern.findFirstMatchIn(addressText).map(_.group(1).trim)
    val statePostal = statePostalPattern.findFirstMatchIn(addressText).map(m => (m.group(1).trim, m.group(2).trim))
    val country = countryPattern.findFirstIn(addressText).map(_.trim)

    for {
      str <- street.map(_ => 1).orElse(Some(0))
      sui <- suite.map(_ => 1).orElse(Some(0))
      cit <- city.map(_ => 1).orElse(Some(0))
      sts <- statePostal.map(_._1).map(_ => 1).orElse(Some(0))
      poc <- statePostal.map(_._2).map(_ => 1).orElse(Some(0))
      cnt <- country.map(_ => 1).orElse(Some(0))
    } yield str + sui + cit + sts + poc + cnt
//
//    val parsedAddress = Map(
//      "street" -> street.getOrElse(""),
//      "suite" -> suite.getOrElse(""),
//      "city" -> city.getOrElse(""),
//      "state" -> statePostal.map(_._1).getOrElse(""),
//      "postal_code" -> statePostal.map(_._2).getOrElse(""),
//      "country" -> country.getOrElse("")
//    )
  }


}
