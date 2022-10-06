// Databricks notebook source
// MAGIC %md
// MAGIC # TODO: add vertretungsregelung event
// MAGIC 
// MAGIC extract via this notebook

// COMMAND ----------

// MAGIC %run ./includes/event_classifications

// COMMAND ----------

// MAGIC %run ./includes/sentence_splitter

// COMMAND ----------

// MAGIC %md
// MAGIC # Base Table

// COMMAND ----------

import org.apache.spark.sql.functions._

val dfPublications = withSplitSentences(spark.table("hreg_events_hrb_only_first_line_cut").join(spark.table("hreg_events_classified_wide"), List("id", "landAbk"))).where("objective_changed")

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC # Extractor

// COMMAND ----------

spark.conf.set("spark.databricks.delta.checkLatestSchemaOnRead", false)

// COMMAND ----------

def withObjective(df: DataFrame) = {
  df
  .withColumn("targetSenArr", expr(getSentenceFilterQueryForEvent("objective_changed")))
  .withColumn("objective", element_at($"targetSenArr", 1))
  .withColumn("objective", regexp_replace($"address", getBlankReplacementRegexForEvent("address"), ""))
}

withObjective(dfPublications).select("globalId", "referenceNumber", "dateOfPublication", "objective").write.mode("overwrite").saveAsTable("hreg_company_objectives")

// COMMAND ----------

// MAGIC %md
// MAGIC # Find Strings

// COMMAND ----------

display(
  spark.table("hreg_events_hrb_only_first_line_cut").join(spark.table("hreg_events_classified_wide"), List("id", "landAbk")).where("NOT objective_changed and publicationBody ILIKE '%gegenstand%'").sample(.1)
)

// COMMAND ----------



// COMMAND ----------

