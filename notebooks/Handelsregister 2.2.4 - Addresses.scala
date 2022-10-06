// Databricks notebook source
// MAGIC %run ./includes/event_classifications

// COMMAND ----------

// MAGIC %run ./includes/sentence_splitter

// COMMAND ----------

// MAGIC %md
// MAGIC # Base Table

// COMMAND ----------

import org.apache.spark.sql.functions._

val dfPublications = withSplitSentences(spark.table("hreg_silver_events_hrb_only_first_line_cut").join(spark.table("hreg_silver_events_classified_wide"), List("globalId"))).where("address")

// COMMAND ----------

// MAGIC %md
// MAGIC # Extractor

// COMMAND ----------

spark.conf.set("spark.databricks.delta.checkLatestSchemaOnRead", false)

// COMMAND ----------

// MAGIC %run ./includes/zip_place_cleaner

// COMMAND ----------

def withAddress(df: DataFrame) = {
  val dfTarget = df
  .withColumn("targetSenArr", expr(getSentenceFilterQueryForEvent("address")))
  .withColumn("address", element_at($"targetSenArr", 1))
  
  replaceColSequentially(dfTarget, "address", "address")
  .withColumn("address", regexp_replace($"address", "^[;:] ?", ""))
  .withColumn("addressZipSplit", split($"address", ","))
  .withColumn("address", trim(element_at($"addressZipSplit", 1)))
  .withColumn("zipAndPlace", trim(element_at($"addressZipSplit", 2)))
  .withColumn("zipCode", trim(regexp_extract($"zipAndPlace", "([0-9]{5})", 1)))
}

spark.sql("DROP TABLE IF EXISTS hreg_silver_company_addresses_from_events_stg")
spark.sql("DROP TABLE IF EXISTS hreg_silver_company_addresses_from_events")

withAddress(dfPublications)
.select("globalId", "referenceNumber", "dateOfPublication", "address", "zipAndPlace", "zipCode")
.write
.mode("overwrite")
.saveAsTable("hreg_silver_company_addresses_stage1_from_events")

stripJunkFromZipAndPlace(spark.table("hreg_silver_company_addresses_stage1_from_events"))
.withColumn("fullAddress", concat_ws(", ", $"address", $"zipAndPlace"))
.join(spark.table("hreg_gold_standardized_reference_numbers"), "referenceNumber")
.join(spark.table("hreg_gold_unifed_reference_numbers"), "stdRefNo")
.select("globalId", "stdRefNoUni", "address", "zipAndPlace", "fullAddress", "zipCode", "dateOfPublication")
.write.mode("overwrite").saveAsTable("hreg_silver_company_addresses_from_events")

// COMMAND ----------

// MAGIC %md
// MAGIC # Detecting first line address changes

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

def detectFirstLineAddressChanges(df: DataFrame) = {
  df
  .select("globalId", "address", "zipAndPlace", "referenceNumber", "dateOfPublication")
  .join(spark.table("hreg_gold_standardized_reference_numbers"), "referenceNumber")
  .join(spark.table("hreg_gold_unifed_reference_numbers"), "stdRefNo")
  .withColumn("fullAddress", concat_ws(", ", $"address", $"zipAndPlace"))
  .withColumn("zipCode", trim(regexp_extract($"zipAndPlace", "([0-9]{5})", 1)))
  .select("globalId", "stdRefNoUni", "address", "zipAndPlace", "fullAddress", "zipCode", "dateOfPublication")
  .withColumn("seq", expr("ROW_NUMBER() OVER (PARTITION BY stdRefNoUni, fullAddress ORDER BY dateOfPublication)"))
  .where("seq = 1")
  .orderBy("stdRefNoUni")
  .drop("seq")
}

spark.sql("DROP TABLE IF EXISTS hreg_silver_company_addresses_from_first_line")

detectFirstLineAddressChanges(spark.table("hreg_silver_company_first_line")).write.mode("overwrite").saveAsTable("hreg_silver_company_addresses_from_first_line")

display(detectFirstLineAddressChanges(spark.table("hreg_silver_company_first_line")))

// COMMAND ----------

// todo: dedupe by global id (pick EVENT change over first line change)
// second round dedupe?
import org.apache.spark.sql.functions._


val dfAddressChanges = spark.table("hreg_silver_company_addresses_from_first_line").withColumn("source", lit("FIRST_LINE"))
.union(spark.table("hreg_silver_company_addresses_from_events").withColumn("source", lit("EVENT")))
.withColumn("fullAddress", regexp_replace($"fullAddress", "straße", "str."))
.withColumn("fullAddress", regexp_replace($"fullAddress", "Straße", "Str."))
.withColumn("seq", expr("row_number() OVER (PARTITION BY stdRefNoUni, fullAddress ORDER BY if(source == 'EVENT', 0, 1))")) // pick distinct addresses preferrably from events
.where("seq = 1")
.orderBy("stdRefNoUni", "dateOfPublication")


//dfAddressChanges.write.saveAsTable("hreg_sivler_company_addresses")

display(dfAddressChanges)

spark.sql("DROP TABLE IF EXISTS hreg_gold_company_address")

dfAddressChanges
.select("stdRefNoUni", "globalId", "dateOfPublication", "fullAddress", "address", "zipCode", "zipAndPlace")
.write.mode("overwrite").saveAsTable("hreg_gold_company_address")

// COMMAND ----------

// good lifecycles: B1202_HRB1265-R3306_HRB54902-B8536_HRB712987

// COMMAND ----------

// MAGIC %md
// MAGIC # Finding strings

// COMMAND ----------


display(
  spark.table("hreg_events_hrb_only_first_line_cut").join(spark.table("hreg_events_classified_wide"), List("id", "landAbk")).where("NOT address and publicationBody ILIKE '%anschrift%' AND NOT publicationBody ILIKE '%bisherige%'").sample(.1)
)