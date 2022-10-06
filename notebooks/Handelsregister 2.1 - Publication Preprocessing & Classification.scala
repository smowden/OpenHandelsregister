// Databricks notebook source
// MAGIC %run ./includes/officer_funcs

// COMMAND ----------

// MAGIC %run ./includes/court_code_funcs

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC # Corporate Only
// MAGIC As a first step lets remove all non capitalized entities such as clubs and associations

// COMMAND ----------

import org.apache.spark.sql.functions._

spark.sql("DROP TABLE IF EXISTS hreg_events_hrb_only")

spark.table("hreg_bronze_publications")
.where("referenceNumber LIKE '% HRB %'")
.where("typeOfPublication != 'andere Bekanntmachungen, zu dem Aktenzeichen suchen: SUCHE'")
.where("publicationBody IS NOT NULL")
.where("typeOfPublication IS NOT NULL")
.where("typeOfPublication != ''")
.withColumn("dateOfPublication", to_date($"dateOfPublication", "dd.MM.yyyy"))
.withColumn("referenceNumber", regexp_replace($"referenceNumber", "Bremerhaven bis zum 31.12.2012", "Bremerhaven"))
//.withColumn("publicationBody", regexp_replace($"publicationBody", ":;", ":")) // check if this doesnt break too much
.withColumn("globalId", concat_ws("_", $"landAbk", $"id")) // add this col for easier joins
.where("dateOfPublication IS NOT NULL")
.orderBy("dateOfPublication")
.write
.mode("overwrite")
.saveAsTable("hreg_silver_publications_hrb_only")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Standardize Reference Numbers
// MAGIC As a first step we build the table of standardized reference numbers.
// MAGIC 
// MAGIC We go from eg Freiburg HRB 490289 to B8536_HRB490289. This means looking up the XJustiz ID of the court and replacing the plain text name with it. Also we replace whitespaces etc.
// MAGIC Later we will use this id to build a "unified id", which spans across court/location changes.

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

val dfReferenceNumbers = spark.table("hreg_silver_publications_hrb_only").select("referenceNumber", "dateOfPublication")

spark.sql("DROP TABLE IF EXISTS hreg_gold_standardized_reference_numbers")

val buildStandardizedRefNoTable = (df: DataFrame) => {
  val makeRefNo = upper(concat($"courtCode", lit("_"), regexp_replace($"hrNo", " ", "_")))

  df
  .withColumn("court", expr(courtFromReferenceNumberExpr))
  .withColumn("courtMatch", findBestMatchingCourtUdf($"court"))
  .withColumn("courtCode", $"courtMatch.code")
  .withColumn("isLegacyCourtCode", $"courtMatch.legacy")
  .withColumn("hrNo", regexp_replace(extractHrNo(lower($"referenceNumber")), " ", ""))
  .withColumn("stdRefNo", makeRefNo)
  .orderBy("dateOfPublication")
  .groupBy("referenceNumber", "stdRefNo")
  .agg(first("courtMatch").as("courtMatch"), min("dateOfPublication").as("referenceNumberFirstSeen"))
}

buildStandardizedRefNoTable(dfReferenceNumbers).write.mode("overwrite").saveAsTable("hreg_gold_standardized_reference_numbers")

display(spark.table("hreg_gold_standardized_reference_numbers"))

// COMMAND ----------

// MAGIC %md
// MAGIC # Event Classification
// MAGIC 
// MAGIC The code below attempts to detect the topic of the publication via simple keyword match. It narrows down the number of publications to look at for future steps.
// MAGIC Can be used as a change history, however the final determination whether the event is what it says it is is only made further down the line.

// COMMAND ----------

// MAGIC %run ./includes/event_classifications

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

val buildWideClassifiedEventsTable = (df: DataFrame) => {
  
  val listToQuery = (likeQuery: List[String]) => likeQuery.map(searchString => s"publicationBody ILIKE '%${searchString}%'").mkString(" OR ")
  val clsList = classifiableEvents.map(e => (e.label, listToQuery(e.likeQuery)))
  
  
  val dfCols = df
  .select("globalId", "publicationBody", "typeOfPublication", "fullText")
  .withColumn("company_founded", $"typeOfPublication" === "Neueintragungen")
  .withColumn("company_to_be_deleted", $"typeOfPublication" === "Löschungsankündigungen")
  .withColumn("illiquid_deletion", $"fullText".like("%Vermögenslosigkeit%gelöscht%"))
  .withColumn("company_deleted", $"typeOfPublication" === "Löschungen")
  .withColumn("company_deletion_official_act", $"typeOfPublication" === "Löschungen von Amts wegen")
  .withColumn("officers_changed", $"publicationBody".rlike("\\*\\d+") || expr("publicationBody RLIKE getTitleTokens()"))
  
  val dfColsDynamic = clsList.foldLeft(df)((df, cls) => df.withColumn(cls._1, expr(cls._2)))
  
  // workarounds
  // sometimes LIKE is not enough and we need regex
  // todo: figgure out a way to not make to so hacky 
  val dfColsDynamicExtra = dfColsDynamic 
  .withColumn("capital_change", $"capital_change" || $"publicationBody".rlike(CAPITAL_REGEX) || $"publicationBody".rlike(CAPITAL_REGEX_2) || $"publicationBody".rlike(CAPITAL_REGEX_FRONT) || $"publicationBody".rlike(CAPITAL_REGEX_FRONT_2)) 

  dfCols
  .join(dfColsDynamicExtra, Seq("globalId"))
  .drop("typeOfPublication")
  .drop("publicationBody")
  .drop("referenceNumber")
  .drop("dateOfPublication")
  .drop("fullText")
}


               
def buildEventsArrayTableFromWide(df: DataFrame) = {
  val nonBoolColumns = Set("globalId", "landAbk", "id")
  val allColumns = df.columns.toSet
  val boolColumns = allColumns.diff(nonBoolColumns)
  val cols = boolColumns.toList
  
  val transformLabelsLongUdf = udf((arr: List[Boolean]) => {
    val labels = arr.zipWithIndex.map { case (element, index) => 
       if(element) {
         cols(index)
       } else{
         ""
       }
    }

    labels.filter(l => l.length > 0)
  })
  
  val boolsDf = df
  .withColumn("array_agg", array(boolColumns.toSeq.map(s => col(s)): _*))
  .withColumn("eventLabels", transformLabelsLongUdf($"array_agg"))
  .select($"globalId", $"eventLabels")
  
  df.join(boolsDf, List("globalId"), "left")
}: DataFrame

spark.sql("DROP TABLE IF EXISTS hreg_events_classified_wide")
spark.sql("DROP TABLE IF EXISTS hreg_events_classified_tall")
spark.sql("DROP TABLE IF EXISTS hreg_events_classified_array")
                
buildWideClassifiedEventsTable(spark.table("hreg_silver_publications_hrb_only")).write.saveAsTable("hreg_silver_events_classified_wide")
//buildEventsArrayTableFromWide(spark.table("hreg_events_classified_wide")).write.saveAsTable("hreg_silver_events_classified_array")
