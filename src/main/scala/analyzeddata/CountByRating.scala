package analyzeddata

import dataframe.ReadDataframe.{movieDataFrame, userDataFrame}
import org.apache.spark.sql.functions._

object CountByRating extends App {

  private val countRatingByMovieName =
    userDataFrame
      .groupBy("MID", "rating")
      .agg(count("rating").as("Total Rating Count"))
      .join(movieDataFrame, "MID", "inner")
      .select("Movie_Name", "rating", "Total Rating Count")
      .orderBy(asc("Movie_Name"), desc("rating"))

  countRatingByMovieName.show(false)

  private val countTopFiveRatingMovies =
    countRatingByMovieName
      .filter(col("rating") === 5).orderBy(desc("Total Rating Count"))

  countTopFiveRatingMovies.show(false)

}
