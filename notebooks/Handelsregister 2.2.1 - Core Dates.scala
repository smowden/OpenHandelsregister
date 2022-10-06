// Databricks notebook source
// MAGIC %md
// MAGIC # Core Dates
// MAGIC Builds a table of the founding, dissolution and first and last seen dates
// MAGIC 
// MAGIC **NOTE**: companies founded before 2007 dont have founding record hence the founded date is null

// COMMAND ----------

import org.apache.spark.sql.functions._ 

val dfBase = spark.table("hreg_silver_publications_hrb_only")
.join(spark.table("hreg_gold_standardized_reference_numbers"), "referenceNumber")
.join(spark.table("hreg_gold_unifed_reference_numbers"), "stdRefNo")
.join(spark.table("hreg_silver_events_classified_wide"), List("id", "landAbk"))

val dfFounded = dfBase
.where("company_founded")
.groupBy("stdRefNoUni")
.agg(min("hreg_silver_publications_hrb_only.dateOfPublication").as("foundedDate"))

val dfDissolved = dfBase
.where("company_deleted OR company_deletion_official_act OR illiquid_deletion")
.groupBy("stdRefNoUni")
.agg(max("hreg_silver_publications_hrb_only.dateOfPublication").as("dissolutionDate"))


val dfFirstSeenDate = dfBase
.groupBy("stdRefNoUni")
.agg(min("hreg_silver_publications_hrb_only.dateOfPublication").as("firstSeenDate"), max("hreg_silver_publications_hrb_only.dateOfPublication").as("lastSeenDate"))

val dfCoreCompanyDates = dfFirstSeenDate.join(dfFounded, Seq("stdRefNoUni"), "left").join(dfDissolved, Seq("stdRefNoUni"), "left")
dfCoreCompanyDates.write.mode("overwrite").saveAsTable("hreg_gold_company_dates")
display(dfCoreCompanyDates)