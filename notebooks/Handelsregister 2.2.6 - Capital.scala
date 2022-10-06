// Databricks notebook source
// MAGIC %run ./includes/event_classifications

// COMMAND ----------

// MAGIC %run ./includes/sentence_splitter

// COMMAND ----------

// MAGIC %md
// MAGIC # Base Table

// COMMAND ----------

import org.apache.spark.sql.functions._

val dfPublications = withSplitSentences(spark.table("hreg_silver_events_hrb_only_first_line_cut").join(spark.table("hreg_silver_events_classified_wide"), "globalId")).where("capital_change")

// COMMAND ----------

// MAGIC %md
// MAGIC # Extractor

// COMMAND ----------

spark.conf.set("spark.databricks.delta.checkLatestSchemaOnRead", false)

// COMMAND ----------

import org.apache.spark.sql.functions.udf

val extractAllCapital = udf((capitalStr: String) => {
  val combinedCapitalRegex = s"($CAPITAL_REGEX)|($CAPITAL_REGEX_2)|($CAPITAL_REGEX_FRONT)|($CAPITAL_REGEX_FRONT_2)".r
  combinedCapitalRegex.findAllIn(capitalStr).toList
})

def withCapital(df: DataFrame) = {
  df
  .withColumn("targetSenArr", expr(getSentenceFilterQueryForEvent("capital_change")))
  .withColumn("capitalSentence", element_at($"targetSenArr", 1))
  .withColumn("capitalSentenceClean", regexp_replace($"capitalSentence", getBlankReplacementRegexForEvent("capital_change"), ""))
  .withColumn("capitalArr", when(!$"capitalSentenceClean".isNull, extractAllCapital($"capitalSentenceClean")))
  .withColumn("capital", element_at($"capitalArr", -1))
  .withColumn("capitalCurrency", regexp_extract($"capital", "(EUR|DM|DEM|GBP)", 1))
  .withColumn("capitalAmountStr", expr("replace(capital, capitalCurrency, '')"))
  .withColumn("capitalAmount", regexp_replace(element_at(split($"capitalAmountStr", ","), 1), "\\.", "").cast("int"))
}

val dfC = withCapital(dfPublications).select("globalId", "referenceNumber", "dateOfPublication", "capital", "capitalSentence", "capitalArr", "capitalCurrency", "capitalAmountStr", "capitalAmount")

display(dfC)

dfC.join(spark.table("hreg_silver_lookup_unified_reference_number"), "referenceNumber")
.select("stdRefNoUni", "globalId", "dateOfPublication", "capitalAmount", "capitalCurrency")
.write.mode("overwrite").saveAsTable("hreg_gold_company_capital")

// COMMAND ----------

// MAGIC %md
// MAGIC # Finding strings

// COMMAND ----------


display(
  spark.table("hreg_silver_events_hrb_only_first_line_cut").join(spark.table("hreg_silver_events_classified_wide"), List("globalId")).where("NOT capital_change and publicationBody NOT LIKE '%§%' AND (publicationBody ILIKE '%stammkapital%' OR publicationBody ILIKE '%grundkapital%' OR publicationBody LIKE '% EUR%' OR publicationBody LIKE '% DM%')").sample(.1)
)