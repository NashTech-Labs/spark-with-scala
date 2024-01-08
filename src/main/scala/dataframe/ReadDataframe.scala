package dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadDataframe{
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("spark-with-scala")
    .getOrCreate()

  private val moviesFilePath = "src/main/resources/movies.csv"
  private val ratingsFilePath = "src/main/resources/user.csv"

  private def readData(filePath: String, schema: String): DataFrame = {
    spark.read.format("csv")
      .option("delimiter", "::")
      .schema(schema)
      .load(filePath)
  }

  val movieDataFrame: DataFrame = readData(moviesFilePath, "MID Int, Movie_Name String, Movie_Type String")
  val userDataFrame: DataFrame = readData(ratingsFilePath, "UserID Int, MID Int, rating Int, timestamp String")

}
