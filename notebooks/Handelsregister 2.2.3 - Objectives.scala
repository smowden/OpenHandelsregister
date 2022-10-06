// Databricks notebook source
// MAGIC %run ./includes/event_classifications

// COMMAND ----------

// MAGIC %run ./includes/sentence_splitter

// COMMAND ----------

// MAGIC %md
// MAGIC # Base Table

// COMMAND ----------

import org.apache.spark.sql.functions._

val dfPublications = withSplitSentences(spark.table("hreg_silver_events_hrb_only_first_line_cut").join(spark.table("hreg_silver_events_classified_wide"), List("globalId"))).where("objective_changed")

// COMMAND ----------

// MAGIC %md
// MAGIC # Extractor

// COMMAND ----------

def withObjective(df: DataFrame) = {
  val dfTarget = df
  .withColumn("targetSenArr", expr(getSentenceFilterQueryForEvent("objective_changed")))
  .withColumn("objective", element_at($"targetSenArr", 1))
  
  replaceColSequentially(dfTarget, "objective", "objective_changed")
}

val dfObjective = withObjective(dfPublications).select("globalId", "referenceNumber", "dateOfPublication", "objective")

display(dfObjective)

dfObjective.join(spark.table("hreg_silver_lookup_unified_reference_number"), "referenceNumber")
.select("stdRefNoUni", "globalId", "dateOfPublication", "objective")
.write.mode("overwrite").saveAsTable("hreg_gold_company_objectives")

// COMMAND ----------

// MAGIC %md
// MAGIC # Find Strings

// COMMAND ----------

display(
  spark.table("hreg_silver_events_hrb_only_first_line_cut").join(spark.table("hreg_silver_events_classified_wide"), List("globalId")).where("NOT objective_changed and publicationBody ILIKE '%gegenstand%'").sample(.1)
)

// COMMAND ----------



// COMMAND ----------

