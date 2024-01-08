package analyzeddata

import dataframe.ReadDataframe._
import org.apache.spark.sql.functions._

object CountMovieType extends App {

  //To split the type of movie and make a row from it
  private val splitDf =
    movieDataFrame
      .withColumn("MovieType", explode(split(col("movie_type"), "\\|")))
      .drop("movie_type")

  //to count the movie type  private
  private val countDF =
    splitDf
      .groupBy("movietype")
      .agg(count("movietype").alias("count"))
      .orderBy(asc("type"))

  //to display the output
  countDF.show(false)
}
