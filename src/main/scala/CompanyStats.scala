import TextHelpers.{findMostCommonAndSimilarText, getAddressScore}
import Utils.{SparkSessionHelpers, DataFrameHelpers, createSparkSession}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{levenshtein => lev, _}

object CompanyStats {

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = createSparkSession(this.getClass.getName)

    import sparkSession.implicits._

    val countries = sparkSession.readCsv("countries.csv").as("c").cache()

    val googleDs = sparkSession.readCsv("google_dataset.csv")
      .withColumnRenamed("name", "company_name")
      .withColumnRenamed("text", "description").as("gg")
      .join(countries, $"gg.country_code" <=> $"c.code" || $"gg.country_name" <=> $"c.name" , "left")
      .withColumn("country_code", coalesce($"gg.country_code", $"c.code"))
      .withColumn("country_name", coalesce($"gg.country_name", $"c.name"))
      .withColumn("phone", regexp_replace($"phone", "[^0-9].", ""))
      .withColumn("address", regexp_replace($"address", "\\d+\\+ years in business · ", ""))
      .drop("raw_phone", "raw_address", "phone_country_code", "code", "name")

    val facebookDs = sparkSession.readCsv("facebook_dataset.csv")
      .withColumnRenamed("name", "company_name")
      .withColumnRenamed("categories", "category").as("fb")
      .join(countries, $"fb.country_code" <=> $"c.code" || $"fb.country_name" <=> $"c.name", "left")
      .withColumn("country_code", coalesce($"fb.country_code", $"c.code"))
      .withColumn("country_name", coalesce($"fb.country_name", $"c.name"))
      .withColumn("phone", regexp_replace($"phone", "[^0-9].", ""))
      .drop("code", "name")

    val websiteDs = sparkSession.readCsv("website_dataset.csv", ";")
      .withColumnRenamed("site_name", "company_name")
      .withColumnRenamed("root_domain", "domain")
      .withColumnRenamed("s_category", "category")
      .withColumnRenamed("main_region", "region_name")
      .withColumnRenamed("main_city", "city")
      .withColumnRenamed("legal_name", "description")
      .withColumn("country_name", lower($"main_country")).as("ws")
      .join(countries, $"ws.country_name" <=> $"c.name", "left")
      .withColumnRenamed("code", "country_code")
      .withColumn("phone", regexp_replace($"phone", "[^0-9].", ""))
      .drop("main_country", "domain_suffix", "language", "tld", "code", "name")

    val getMostCommonAndSimilarTextUdf = udf {(text1: Option[String], text2: Option[String], text3: Option[String]) =>
      val texts = text1.map(_.split(",").map(_.trim)).toList.flatten ++ text2.map(_.split("\\|").map(_.trim)).toList.flatten ++ text3.toList
      texts.headOption.map(_ => findMostCommonAndSimilarText(texts))
    }

    val getAddressUdf = udf {(address1: Option[String], address2: Option[String]) =>
      val adr1Score = address1.flatMap(getAddressScore).getOrElse(0)
      val adr2Score = address2.flatMap(getAddressScore).getOrElse(0)
      if (adr2Score > adr1Score) {
        address2
      } else {
        address1
      }
    }

    googleDs.join(facebookDs, lev(googleDs("company_name"), facebookDs("company_name")) <= 50 &&
        googleDs("domain") <=> facebookDs("domain") &&
        googleDs("phone") <=> facebookDs("phone") &&
        googleDs("country_code") <=> facebookDs("country_code"),
      "fullouter")
      .join(websiteDs, lev(googleDs("company_name"), websiteDs("company_name")) <= 50 &&
        googleDs("domain") <=> websiteDs("domain") &&
        googleDs("phone") <=> websiteDs("phone") &&
        googleDs("country_code") <=> websiteDs("country_code"),
        "fullouter")
      .select(
        getMostCommonAndSimilarTextUdf(googleDs("company_name"), facebookDs("company_name"), websiteDs("company_name")).as("company_name"),
        coalesce(googleDs("domain"), facebookDs("domain"), websiteDs("domain")).as("domain"),
        getMostCommonAndSimilarTextUdf(googleDs("category"), facebookDs("category"), websiteDs("category")).as("category"),
        coalesce(googleDs("country_code"), facebookDs("country_code"), websiteDs("country_code")).as("country_code"),
        coalesce(googleDs("country_name"), facebookDs("country_name"), websiteDs("country_name")).as("country_name"),
        coalesce(googleDs("region_name"), facebookDs("region_name"), websiteDs("region_name")).as("region_name"),
        coalesce(googleDs("city"), facebookDs("city"), websiteDs("city")).as("city"),
        getAddressUdf(googleDs("address"), facebookDs("address")).as("address"),
        coalesce(googleDs("phone"), facebookDs("phone"), websiteDs("phone")).as("phone"),
        facebookDs("email").as("email"),
        facebookDs("link").as("link"),
        coalesce(googleDs("zip_code"), facebookDs("zip_code")).as("zip_code"),
        facebookDs("page_type").as("page_type"),
        coalesce(googleDs("description"), facebookDs("description"), websiteDs("description")).as("description")
      )
      .writeParquet("sites")
//      .show(100, false)
    val sitesDs = sparkSession.readParquet("sites")

    sitesDs.printSchema()
    sitesDs.show(100, false)

  }
}


