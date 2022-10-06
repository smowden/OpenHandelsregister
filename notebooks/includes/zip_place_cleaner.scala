// Databricks notebook source
// MAGIC %md
// MAGIC # Zip and place junk remover
// MAGIC Often times there is a dot or something indicating the end of the sentence missing (as the sentence splitter is not perfect). Hence zipAndPlace becomes too long as it contains fragments of the next sentence.
// MAGIC Fortunately we can fetch a zipcode db from Geonames (here: http://www.geonames.org/export/zip/DE.zip) and try to partially match on zip code + city to get the correct string.
// MAGIC Note: The geonames table is loaded into hreg_zipcodes via UI atm, add a way to load it via code. Relevant labels are: zip_code and place.
// MAGIC 
// MAGIC Once the cleaning is done we update the table in place.

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

def stripJunkFromZipAndPlace(df: DataFrame) = {
  val dfZipPlace = spark.table("hreg_zipcodes").withColumn("correctZipPlace", concat_ws(" ", $"zip_code", $"place")).select($"correctZipPlace", $"zip_code".as("correctZipCode"))
  
  val dfZipWithJunk = df.join(dfZipPlace, expr("correctZipCode = zipCode AND zipAndPlace = correctZipPlace"), "left")
  
  val dfNoJunk = dfZipWithJunk.where("correctZipPlace IS NOT NULL")
  val dfJunk = dfZipWithJunk.where("correctZipPlace IS NULL").drop("correctZipPlace", "correctZipCode")
  
  
  val dfWithoutJunk = dfJunk
  // important, join on zip first as it is correct because its easy to detect. this speeds up the process immensly as it cuts down on the search space alot
  .join(dfZipPlace, expr("correctZipCode = zipCode AND like(zipAndPlace, concat(correctZipPlace, '%'))"), "inner")
  .withColumn("zipAndPlace", coalesce($"correctZipPlace", $"zipAndPlace"))
  
  dfNoJunk
  .union(dfWithoutJunk)
  .drop("correctZipPlace", "correctZipCode")
}
