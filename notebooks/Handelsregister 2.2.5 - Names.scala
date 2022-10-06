// Databricks notebook source
// MAGIC %run ./includes/event_classifications

// COMMAND ----------

// MAGIC %run ./includes/sentence_splitter

// COMMAND ----------

// MAGIC %md
// MAGIC # Base Table

// COMMAND ----------

import org.apache.spark.sql.functions._

val dfPublications = withSplitSentences(spark.table("hreg_silver_events_hrb_only_first_line_cut").join(spark.table("hreg_silver_events_classified_wide"), List("globalId"))).where("name_change")

// COMMAND ----------

// MAGIC %md
// MAGIC # Extractor

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

def withNameChange(df: DataFrame) = {
  val dfTarget = df
  .withColumn("targetSenArr", expr(getSentenceFilterQueryForEvent("name_change")))
  .withColumn("nameSentence", element_at($"targetSenArr", 1))
  .withColumn("name", $"nameSentence")
  
  replaceColSequentially(dfTarget, "name", "name_change")
  .withColumn("name", regexp_replace($"name", ".*Firma:", ""))
  .withColumn("name", regexp_replace($"name", "^: ", "")) // leading :
  .withColumn("name", regexp_replace($"name", "\\.$", "")) // trailing dot
  .withColumn("name", regexp_replace($"name", "\\(kurz:.*", ""))
  .withColumn("name", regexp_replace($"name", "\\):.*", ""))
  .withColumn("name", regexp_replace($"name", "(?<=GmbH)\\..*", ""))
  .withColumn("name", regexp_replace($"name", "\\. geändert nun:.*", ""))
  .withColumn("name", regexp_replace($"name", ", Anschrift bisher:.*", ""))
  .withColumn("name", regexp_replace($"name", "\\) nunmehr:.*", ""))
  .withColumn("name", regexp_replace($"name", ", (\\w+):.*", ""))
  //.withColumn("name", regexp_replace($"name", "(Nach Firmenänderung nunmehr:|Geändert, nun:|Geändert, nun:|Nach Änderung:|Geändert, jetzt:|Geändert, nunmehr:|Nach Änderung nunmehr:)", ""))
  .withColumn("name", trim($"name"))
  .withColumn("name", expr("nullif(name, '')"))
}

val dfR = withNameChange(dfPublications).select("globalId", "referenceNumber", "typeOfPublication", "dateOfPublication", "name", "nameSentence", "sentences", "publicationBody")//.where("name LIKE '%:%' AND name NOT LIKE '%Zweigniederlassung%'")
display(
  dfR
)

spark.sql("DROP TABLE IF EXISTS hreg_gold_company_names")
dfR
.join(spark.table("hreg_silver_lookup_unified_reference_number"), "referenceNumber")
.select("stdRefNoUni", "globalId", "dateOfPublication", "name")
.write.mode("overwrite").saveAsTable("hreg_silver_changes_from_event")

// COMMAND ----------

spark
.table("hreg_silver_company_first_line").select("globalId", "dateOfPublication", "referenceNumber", "name")
.join(spark.table("hreg_silver_lookup_unified_reference_number"), "referenceNumber")
.select("stdRefNoUni", "globalId", "dateOfPublication", "name")
.union(spark.table("hreg_silver_changes_from_event").select("stdRefNoUni", "globalId", "dateOfPublication", "name"))
.withColumn("seq", row_number().over(Window.partitionBy($"stdRefNoUni", $"name").orderBy($"dateOfPublication")))
.where("seq = 1")
.drop("seq")
.write.mode("overwrite").saveAsTable("hreg_gold_company_names")

// COMMAND ----------

// MAGIC %md
// MAGIC # Finding strings

// COMMAND ----------


display(
  spark.table("hreg_silver_events_hrb_only_first_line_cut").join(spark.table("hreg_silver_events_classified_wide"), List("globalId")).where("NOT name_change and (publicationBody ILIKE 'Name:%' OR publicationBody ILIKE '%firma%lautet%' OR publicationBody ILIKE '%firma%lautet%')").where("typeOfPublication == 'Veränderungen'")
)