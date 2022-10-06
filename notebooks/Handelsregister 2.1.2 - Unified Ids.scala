// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC # Unified Ids
// MAGIC 
// MAGIC As companies change register courts they get issued a new local id. We want to keep track of those so we dont register them as a new company every time

// COMMAND ----------

// MAGIC %run ./includes/court_code_funcs

// COMMAND ----------

// MAGIC %md
// MAGIC # Base Table
// MAGIC 
// MAGIC we grab all events that document a court change

// COMMAND ----------

val dfBase = spark.table("hreg_silver_publications_hrb_only")
.join(spark.table("hreg_gold_standardized_reference_numbers"), "referenceNumber")
.join(spark.table("hreg_silver_events_classified_wide"), List("globalId"))
.where("court_moved")
.select("globalId", "stdRefNo", "publicationBody", "hreg_silver_publications_hrb_only.dateOfPublication")

// COMMAND ----------

// MAGIC %md
// MAGIC # Extracting reference numbers from court change events
// MAGIC When a court change occurs the new court issues a new number and the old court published an event that states the court change.
// MAGIC 
// MAGIC Here we grab those events and attempt to extract the new court number as best we can

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame

def withCourtChangeInfo(df: DataFrame) = {
  
  val aliasToCodeList = getCourtCodeReplacementsList(aliasesWithCourts)
  
  def extractBetweenBrackets(targetCol: Column) = {
      regexp_extract(targetCol, "\\((.*hr[ab].*?)\\)( nach|\\.)?", 1)
  } 
  
  def isFormer(targetCol: Column) = {
    // wenn: nun, jetzt => new
    // wenn: bisher => former
    // else: unknown
    
    // other way:
    // this court 
  }
  
  def dropFirstSentence(targetCol: Column) = {
    // contains company name and address usally. not necessary here
    regexp_replace(targetCol, "^(.*?)\\)\\.", "")
  }
  
  def extractCourt(targetCol: Column) = {
    // split on , or hr
    // first part
    // split spaces and take last word
    val stripStrings = "( )?(jetzt|nun|bisher|amt\\w+|ag|vormals|registergericht|aktenzeichen|jetzt|nun)[: ]"
    val stripSpecialChars = "([:;])"
    val semiClean = regexp_replace(element_at(split(targetCol, "hr([ab]|\\s[ab])"), 1), stripStrings, " ")
    regexp_replace(semiClean, stripSpecialChars, " ")
  }
  
  val replaceDanglingDash = regexp_replace($"extractedCourt2", " -$", "")
  
  def lastDitchExtractor(df: DataFrame) = {
    df.withColumn("replacedCourt", massReplace($"changedCourt2", aliasToCodeList))
      .withColumn("lastDitchEffortCourtCode", regexp_extract($"replacedCourt", ".*Q([A-Z][0-9]+[A-Z]?)E.*", 1)) // not sure where the Q..E comes from but it is a nice sideeffect (maybe Regexp.quote in util?)
      .withColumn("extractedHrNo2", trim(extractHrNo($"changedCourt2")))
      .withColumn("changeStdRefNo", when(length($"extractedHrNo2") > 1 && length($"lastDitchEffortCourtCode") > 3, upper(concat($"lastDitchEffortCourtCode", lit("_"), regexp_replace($"extractedHrNo2", " ", "")))))
      .withColumn("confidence", lit(0.7))
  }
  
  def courtExtractorArrayApproach(df: DataFrame) = {
    df.withColumn("tmpLower", lower($"publicationBody"))
      .withColumn("targetSenArr", expr("split(tmpLower, '(sitz)')"))
      .withColumn("changeSentence", expr("""
         CASE WHEN size(targetSenArr) > 2 THEN element_at(filter(targetSenArr, s -> (like(s, '%verlegt%'))), 1)
         ELSE element_at(targetSenArr, 2) END
      """))
      .withColumn("changeSentence2", element_at(expr("split(changeSentence, '(persönlich|inhaber)')"), 1))
      .withColumn("courtActuallyChanged", $"changeSentence2".like("% hr_ %") || $"changeSentence2".like("% hr _ %")) // move this upstream if possible, if this is false then its just an address change
      .withColumn("changedCourt2", extractBetweenBrackets($"changeSentence2"))
      .withColumn("extractedCourt2", trim(extractCourt($"changedCourt2")))
      .withColumn("extractedCourt2", trim(regexp_replace($"extractedCourt2", ",", "")))
      .withColumn("extractedCourt2",  // split on ( in case we have multiple brackets, take the last one
        when(col("extractedCourt2").like("%)%(%"), element_at(reverse(split($"extractedCourt2", "\\(")), 1))
        .otherwise($"extractedCourt2")
      )
      .withColumn("extractedCourt2", trim(replaceDanglingDash))
      .withColumn("extractedHrNo2", trim(extractHrNo($"changedCourt2")))
      //.withColumn("isValidExtractedCourt", col("extractedCourt2").rlike("\\w[0-9]+(\\w)?"))
      .withColumn("changeCourtInfo", findBestMatchingCourtUdf($"extractedCourt2"))
      //.withColumn("changeStdRefNo", upper(coalesce(col("extractedCourt2"), lit("_"), regexp_replace($"extractedHrNo2", " ", ""))))
      //.drop("tmpLower", "startStr")
      .withColumn("changeStdRefNo", 
        when(col("changeCourtInfo.score") > 0.7, upper(concat($"changeCourtInfo.code", lit("_"), regexp_replace($"extractedHrNo2", " ", ""))))
      )
      .withColumn("confidence", $"changeCourtInfo.score")
    //blank fields for union
      .withColumn("replacedCourt", lit(""))
      .withColumn("lastDitchEffortCourtCode", lit(""))
  }
  
  val dfApproach1 = courtExtractorArrayApproach(df)
  val dfApproach1Success = dfApproach1.where("confidence >= 0.7")
  val dfApproach1Fail = dfApproach1.where("confidence < 0.7")
  val dfApproach2 = lastDitchExtractor(dfApproach1Fail)
                  
  dfApproach1Success.union(dfApproach2)

  //.withColumn("changeStdRefNo", upper(concat($"extractedCourt2", lit("_"), regexp_replace($"extractedHrNo2", " ", ""))))
  
  
}


spark.sql("DROP TABLE IF EXISTS hreg_silver_events_court_changes;")
val dfCourts = withCourtChangeInfo(dfBase).select("globalId", "stdRefNo", "changeStdRefNo", "dateOfPublication")

dfCourts.write.mode("overwrite").saveAsTable("hreg_silver_events_court_changes")
display(spark.table("hreg_silver_events_court_changes"))


//dfCourts.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("tmp_changed_courts")

// COMMAND ----------

// MAGIC %md
// MAGIC # Using GraphFrames to connect all reference numbers

// COMMAND ----------

import org.graphframes.GraphFrame
import org.apache.spark.sql.functions._

def buildRefNoChangeGraph(dfAllRefs: DataFrame, dfChangeRefTable: DataFrame) = {
  // could be a recursive query instead but unfortunatly those are not supported
  val allRefNos = dfAllRefs.select("stdRefNo")
  val changeRefs = dfChangeRefTable.select("changeStdRefNo")
  val refNoUnion = allRefNos.union(changeRefs).distinct()
  val vertices = refNoUnion.select($"stdRefNo".alias("id"), $"stdRefNo")
  val edges =  dfChangeRefTable.select($"stdRefNo".alias("src"), $"changeStdRefNo".alias("dst"), lit("change"))
  sc.setCheckpointDir("/tmp")
  val g = GraphFrame(vertices, edges)
  val result = g.connectedComponents.run()
  result
}

buildRefNoChangeGraph(spark.table("hreg_gold_standardized_reference_numbers"), spark.table("hreg_silver_events_court_changes")).write.mode("overwrite").saveAsTable("hreg_silver_unified_reference_numbers_ids")

display(
  spark.table("hreg_silver_unified_reference_numbers_ids")
)



// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Building the lookup table
// MAGIC for each standardized reference number we generate the unified number by combining all the standard reference numbers
// MAGIC based on this table we can later lookup the unified number based on the standardized number

// COMMAND ----------


val dfRefWithDates = spark.table("hreg_gold_standardized_reference_numbers").select($"stdRefNo", $"referenceNumberFirstSeen".alias("dateOfPublication"))
val dfRefChangesWithDates = spark.table("hreg_silver_events_court_changes").select("changeStdRefNo", "dateOfPublication")
val dfRefNoUnion = dfRefWithDates.union(dfRefChangesWithDates).groupBy("stdRefNo").agg(min("dateOfPublication")).select($"stdRefNo", $"min(dateOfPublication)".alias("firstDate"))
val dfUnifiedCourtCodeIds = spark.table("hreg_silver_unified_reference_numbers_ids")

val dfChangesWithDates = dfRefNoUnion.join(dfUnifiedCourtCodeIds, dfUnifiedCourtCodeIds.col("stdRefNo") === dfRefNoUnion.col("stdRefNo"), "left").orderBy("firstDate")
val dfAggIds = dfChangesWithDates
                .groupBy("component")
                .agg(collect_list($"id"))
                .drop("component")
                .select($"collect_list(id)".alias("stdRefNoUnifiedArr"))
                .withColumn("stdRefNoUnifiedArr", array_distinct($"stdRefNoUnifiedArr"))


val multiRefNoDf = dfAggIds
                    .withColumn("stdRefNo", explode($"stdRefNoUnifiedArr"))
                    .withColumn("stdRefNoUni", concat_ws("-", $"stdRefNoUnifiedArr"))
                    .withColumn("stdRefNoUni", coalesce($"stdRefNoUni", $"stdRefNo"))
                    .drop("stdRefNoUnifiedArr")
//.write.mode("overwrite").saveAsTable("unified_std_ref_nos")

multiRefNoDf.write.mode("overwrite").saveAsTable("hreg_gold_unifed_reference_numbers")
display(multiRefNoDf)


// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS hreg_gold_unified_reference_numbers")
spark.table("hreg_gold_unifed_reference_numbers")
.select("stdRefNoUni")
.distinct
.write.saveAsTable("hreg_gold_unified_reference_numbers")

// COMMAND ----------

// MAGIC %md
// MAGIC #Unprocessed refrence number to unified table

// COMMAND ----------



spark.table("hreg_gold_standardized_reference_numbers").join(spark.table("hreg_gold_unifed_reference_numbers"), "stdRefNo").select("referenceNumber", "stdRefNoUni").write.mode("overwrite").saveAsTable("hreg_silver_lookup_unified_reference_number")