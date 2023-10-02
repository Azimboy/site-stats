import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Utils {

  implicit class SparkSessionHelpers(sparkSession: SparkSession) {
    def readCsv(fileName: String, delimiter: String = ","): DataFrame = {
      val filePath = getClass.getClassLoader.getResource(fileName).getPath
      sparkSession
        .read
        .option("header", true)
        .option("delimiter", delimiter)
        .option("multiline", value = true)
        .csv(filePath)
    }

    def readParquet(tableName: String): DataFrame = {
      sparkSession.read.parquet(s"./target/$tableName")
    }
  }

  implicit class DataFrameHelpers(dataFrame: DataFrame) {
    def writeParquet(tableName: String): Unit = {
      dataFrame.repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .parquet(s"./target/$tableName")
    }
  }

  def createSparkSession(appName: String): SparkSession = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.set("spark.sql.session.timeZone", "UTC")
    sparkConf.set("spark.sql.warehouse.dir", "./target")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    SparkSession.builder()
      .config(sparkConf)
      .appName(appName)
      .getOrCreate()
  }

}