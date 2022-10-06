// Databricks notebook source
// MAGIC %run ./includes/officer_funcs

// COMMAND ----------

// MAGIC %run ./includes/sentence_splitter

// COMMAND ----------

import org.apache.spark.sql.functions._

spark.sql("DROP TABLE IF EXISTS hreg_silver_people_stage0");
spark.sql("DROP TABLE IF EXISTS hreg_silver_people_stage1");

// todo: merge here with change strings from updates
// TODO: find all mit der befugniss.. strings and use them to split (split after end of sentence)


val dfExtractedPeopleStrAndSplit = spark.table("hreg_silver_events_hrb_only_first_line_cut").join(spark.table("hreg_silver_events_classified_wide"), "globalId").where("officers_changed")
.where(expr("officers_changed"))
.withColumn("tmpPeopleStr", regexp_extract($"publicationBody", getPeopleStrStartRegex(getTitleTokens()), 0))
.withColumn("peopleStr", element_at(
  split(
    $"tmpPeopleStr", 
    "(?<!\\()(Rechtsform|Gesellschaft mit beschränkter Haftung|Kommanditgesellschaft|Geschäftsanschrift|Der|Die(?! Eintragung)|Das|Mit|Nicht eingetragen|Entstanden|Satzung|Einzelkaufmännisches|Er|Sie|Durch|Rechtsverhaeltnis|Für|Als|#DV\\.auswahl)[\\s\\.;:]" //Aktiengesellschaft (can be company name but also description)
  ), 1)
)
.withColumn("peopleStr", regexp_replace($"peopleStr", ":;", " :"))// buggy semicolon, eg Vorstand: Vorstand:; 1. Evertz, Frank-Peter, Berlin
.withColumn("peopleStr", regexp_replace($"peopleStr", ";  :", "; "))
.withColumn("peopleStr", regexp_replace($"peopleStr", "; \\*", " *"))
.withColumn("peopleStr", regexp_replace($"peopleStr", "; /", " /"))
.withColumn("officerStrList", splitOfficer($"peopleStr", getTitleTokensUdf()))
.withColumn("officerStrList", postprocessDoubleColonStrings($"officerStrList"))
.write.mode("overwrite").saveAsTable("hreg_silver_people_stage0")

/*val dfPeopleStrSplit = spark.table("hreg_people_stage0")
.select("globalId", "peopleStr")
.sample(.01)
.withColumn("officerStrList", splitOfficer($"peopleStr", getTitleTokensUdf()))
.withColumn("officerStr", expr("posexplode(officerStrList) AS (os1, i)"))*/
//.withColumn("officerStrPotentialSplit", when(expr("officerStr LIKE '%:%:%'"), postprocessDoubleColonStrings($"officerStr")).otherwise(array($"officerStr")))
//.withColumn("officerSplit2Explode", expr("posexplode(officerStrPotentialSplit) AS (os2, j)"))



val dfExtractedOfficersStructured = spark.table("hreg_silver_people_stage0").withColumn("extractedOfficers", expr("transform(officerStrList, (e, i) -> extractOfficerOrCompany(e))"))
.write.mode("overwrite").saveAsTable("hreg_silver_people_stage1")


display(spark.table("hreg_silver_people_stage1"))


// COMMAND ----------

import org.apache.spark.sql.functions._

display(
  spark.table("hreg_silver_people_stage1").withColumn("officerStr", explode($"officerStrList")).select("officerStrList", "peopleStr", "officerStr").orderBy("officerStr")
)

// COMMAND ----------

import org.apache.spark.sql.functions._

display(
  spark.table("hreg_silver_people_stage1").withColumn("officerStr", explode($"officerStrList")).select("officerStrList", "peopleStr", "officerStr").groupBy("officerStr").count().orderBy(desc("count"))
)

// COMMAND ----------

import org.apache.spark.sql.functions._
val POSITION_FORMER_REGEX = "(Nicht mehr|War|war |Ausgeschieden als|Ausgeschieden|abberufen|ausgeschieden|nicht mehr|erloschen).*"
val POSITION_FORMER_ILIKE = "SOME('%Nicht mehr%', '%War%', '%Ausgeschieden%', '%abberufen%', '%ausgeschieden%', '%nicht mehr%', '%erloschen%')"

val positions = List("Geschäftsführer", "Geschäftsführerin", "Prokurist", "Vorstand", "Liquidator", "Liquidatorin", "Director", "Vorstandsvorsitzender", "Ständiger Vertreter", "Vorstandsmitglied", "Direktor", "Geschäftsführer", "Geschäftsführender Direktor", "geschäftsführender Direktor", "director", "Verwaltungsratsmitglied", "Abwickler", "Notgeschäftsführer", "Ständiger Vertreter der Zweigniederlassung", "Geschäftsleiter", "Verwaltungsrat", "Geschäftsführer (director)", "Mitglied des Verwaltungsrates", "Direktorin", "Geschäftsführer und ständiger Vertreter der Zweigniederlassung", "ständiger Vertreter der Zweigniederlassung", "Geschäftsführer (Director)", "Präsident", "Prokuristin", "Persönlich haftender Gesellschafter", "Ständiger Vertreter für die Tätigkeit der Zweigniederlassung", "Generaldirektor", "Vorsitzender", "Director und ständiger Vertreter der Zweigniederlassung", "Ständige Vertreterin", "Hauptbevollmächtigter", "Aufsichtsrat", "Verwaltungsratmitglied", "Leiter der Zweigniederlassung", "Vorsitzender des Vorstandes", "Vorstandsvorsitzende", "Aufsichtsratsvorsitzender", "Representative Director", "Director und ständiger Vertreter", "Verwaltungsrätin", "Notliquidator", "Directeur", "Sonstiger Vertreter", "Gerichtlich bestellte Vertreter", "Gesschäftsführerin", "Secretary", "Geschäfttsführerin", "GF", "stellv. Aufsichtsratvors.", "Ständiger Vertreterin der Zweigniederlassung", "Geschäftsfüherin", "Nachtragsliquidator", "Gesellschaftssekretär", "empfangsberechtigte Person", "Gesamtprokura", "Einzelprokura", "Prokura", "geschäftsführer", "ständiger Vertreter", "Inhaber", "liquidatorin", "liquidator", "direktor", "Inhaber", "Gesellschafter", "vorstand", "prokurist", "Geschäfsführerin", "Besonderer Vertreter", "Vizepräsident", "ständige Vertreterin", "Verwalter", "ständige Vertreterin der Zweigniederlassung", "Ständiger Verteter", "Prokuren", "Geschäftführerin", "präsident", "Mitglied des Leitungsorgans", "Mitglied der Geschäftsleitung", "Mitglied des Verwaltungrates", "stellvertretender Vorstand", "Generalbevollmächtigter", "Vertreter der Gesellschaft", "Vice President", "Vertreter der Zweigniederlassung", "Partner", "Gescäftsführerin", "Gechäftsführerin", "Insolvenzverwalter", "Gschäftsführerin", "Mitglied der Geschäftsleitung", "Vorsitzende", "vorläufiger Insolvenzverwalter", "Geschäftsfühererin", "Sekretär", "Geschäfts- führer", "Vewaltungsratsmitglied", "bevollmächtigter Gesellschaftsangehöriger", "Finanzleiter", "Stellv. Sekretär", "Mitglied des Verwaltungrates", "Geschäftsfürerin", "Geschäftsfürerin", "Vice President", "Stellvertreterin", "Officer", "Ständgier Vertreter der Zweigniederlassung", "Niederlassungsleiter", "Voirstandsmitgliedern").distinct.sortBy(_.length).reverse

case class PositionMatch(position: String, idx: Int)

val findPositions = udf((positionCol: String) => {
  val foundPositions = positions.filter(pos => positionCol.contains(pos))
  val indexPositions = foundPositions.map(pos => positionCol.indexOf(pos))
  (foundPositions zip indexPositions).sortBy(t => t._2).map(t => PositionMatch tupled t)
})


// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame



val withBackfilledPositions = (df: DataFrame) => {
  // when multiple people are listed for a position, only the first one is prefixed with the position. the other ones are just names. this backfills the position string for all those people
  val window = Window
      .partitionBy("globalId")
      .orderBy("pos")

  df
  .withColumn("position", last("position", true).over(window)) // check if it works for multiple roles in same statement (eg Nicht mehr geschäftsführer: bla, Geschäftsführerer: bla; bla; Prouka: bla) eg 403065, be
}


// COMMAND ----------

import org.apache.spark.sql.functions._

spark.sql("DROP TABLE IF EXISTS hreg_people_stage1_explode")
// explode extraction table
spark.table("hreg_silver_people_stage1")
.select($"globalId", $"dateOfPublication", $"referenceNumber", posexplode($"extractedOfficers"))
.select("globalId", "dateOfPublication", "referenceNumber", "pos", "col.*")
.write.mode("overwrite")
.saveAsTable("hreg_silver_people_stage1_explode")

spark.sql("DROP TABLE IF EXISTS hreg_people_stage2_strip_changes")
// strip out change strings, note this means when a title changes for the same person, we have to change the position for him
spark
.table("hreg_silver_people_stage1_explode")
.where("position != 'PERSON_EXTRACTION_ERROR'")
.withColumn("oPosition", $"position")
.withColumn("position", element_at(split($"position", "nunmehr"), -1))
.withColumn("position", element_at(split($"position", "jetzt"), -1))
.withColumn("position", element_at(split($"position", ", nun"), -1))
.withColumn("position", element_at(split($"position", ", weiterhin"), -1))
.withColumn("position", when($"position" === "", lit(null)).otherwise($"position"))
.write.mode("overwrite")
.saveAsTable("hreg_silver_people_stage2_strip_changes")

withBackfilledPositions(spark.table("hreg_silver_people_stage2_strip_changes"))
.withColumn("position", when($"position".isNull, "UNKNOWN").otherwise($"position"))
.withColumn("position", when(expr("position ILIKE 'Geschäftsf%'"), "Geschäftsführer").otherwise($"position")) // standerdize Geschäftsführer typos and Geschäftsführerin
.write.mode("overwrite")
.saveAsTable("hreg_silver_people_stage3_backfilled_positions")
// build dfs

val dfJunk = spark
.table("hreg_silver_people_stage3_backfilled_positions")
.withColumn("foundPositions", findPositions($"position"))
.where("size(foundPositions) > 0 OR position IS NULL")

val dfBase = spark.table("hreg_silver_people_stage3_backfilled_positions")
.withColumn("foundPositions", findPositions($"position"))
.where("size(foundPositions) > 0 OR position IS NULL")
.withColumn("appointed", lit(false))
.withColumn("former", lit(false))
.withColumn("startDate", lit(null))
.withColumn("endDate", lit(null))


// appointments
val dfAppointed = dfBase
.where(s"position ILIKE '%bestellt%' AND NOT position RLIKE '$POSITION_FORMER_REGEX'")
//.withColumn("foundTitles", findPosition($"position"))
//.withColumn("positionFound", element_at($"foundTitles", 1))

val dfAppointedExtraCols = dfAppointed
.withColumn("appointed", lit(false))
.withColumn("startDate", $"dateOfPublication")
.withColumn("foundPositionObj", element_at($"foundPositions", 1))
.withColumn("foundPosition", $"foundPositionObj.position")
//.groupBy("position", "foundPosition")
//.count().orderBy(desc("count"))


val dfFormer = dfBase
.where(s"position RLIKE '$POSITION_FORMER_REGEX' AND NOT position ILIKE '%bestellt%'")

val dfFormerExtraCols = dfFormer
.withColumn("foundPositionObj", element_at($"foundPositions", 1))
.withColumn("foundPosition", $"foundPositionObj.position")
.withColumn("endDate", $"dateOfPublication")
.withColumn("former", lit(true))
//.groupBy("position", "foundPosition")
//.count().orderBy(desc("count"))


val dfRest = dfBase
.except(dfAppointed)
.except(dfFormer)
.withColumn("foundPositionObj", element_at($"foundPositions", 1))
.withColumn("foundPosition", $"foundPositionObj.position")
.withColumn("startDate", $"dateOfPublication")
//.where("foundPosition IS NULL")
//.groupBy("foundPosition")
//.count().orderBy(desc("count"))

display(dfRest)



// COMMAND ----------

import org.apache.spark.sql.functions.col

spark.sql("DROP TABLE IF EXISTS hreg_silver_people_stage4_all_positions")
val positionColsList = List("globalId", "referenceNumber", "dateOfPublication", "pos", "foundPosition", "former", "appointed", "startDate", "endDate", "firstName", "lastName", "maidenName", "birthDate", "city")
val positionCols = positionColsList.map(col)
val dfPositions = dfRest.select(positionCols: _*)
.union(dfFormerExtraCols.select(positionCols: _*))
.union(dfAppointedExtraCols.select(positionCols: _*))
.join(spark.table("hreg_silver_lookup_unified_reference_number"), "referenceNumber")
.withColumn("id", monotonically_increasing_id())
.withColumn("isEndBinary", when($"endDate".isNull, lit(0)).otherwise(lit(1)))
.withColumn("startOrEndDate", coalesce($"startDate", $"endDate"))
.write.mode("overwrite").saveAsTable("hreg_silver_people_stage4_all_positions")
display(spark.table("hreg_silver_people_stage4_all_positions"))


val dfPositionsDeduped = spark.table("hreg_silver_people_stage4_all_positions")
.withColumn("seq", row_number().over(Window.partitionBy("stdRefNoUni", "startOrEndDate", "firstName", "lastName").orderBy("isEndBinary")))
.where("seq = 1")
.write.mode("overwrite").saveAsTable("hreg_silver_people_stage4_deduped_positions")

val w = Window.partitionBy("stdRefNoUni", "firstName", "lastName").orderBy("startOrEnd")//.rowsBetween(1, Window.currentRow)
val dfPositionsWithEnd = spark.table("hreg_silver_people_stage4_deduped_positions")
.withColumn("startOrEnd", coalesce($"startDate", $"endDate"))
.withColumn("endDate", when(!$"startDate".isNull, lead($"startOrEnd", 1).over(w)).otherwise($"endDate"))
.write.mode("overwrite").saveAsTable("hreg_silver_people_stage5_positions_w_start_and_end")

val dfOpenPositions = spark.table("hreg_silver_people_stage5_positions_w_start_and_end")
.where("startDate IS NOT NULL")
.select("stdRefNoUni", "globalId", "dateOfPublication", "foundPosition", "startDate", "endDate", "firstName", "lastName", "maidenName", "birthDate", "city")
.where("firstName NOT ILIKE SOME('%Liquidator%', '%vertritt%')") // buggy records
.write.mode("overwrite").saveAsTable("hreg_gold_companies_positions")

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC # TODO: potentially Buggy records

// COMMAND ----------

display(spark.table("hreg_people_stage5_positions_w_start_and_end").where("birthDate IS NULL AND length(firstName) > 15"))

// COMMAND ----------

// MAGIC %md
// MAGIC # TODO: merge overlapping positions 
// MAGIC in some cases such as court moves a position is restated. this terminates the old position and starts an identical position again. this is redundant.
// MAGIC see gaps and islands problem

// COMMAND ----------

// MAGIC %md
// MAGIC # TODO companies quite broken

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM hreg_people_stage1_explode WHERE isCompany

// COMMAND ----------

// MAGIC %md
// MAGIC # todos
// MAGIC - handle position changes, "nicht mehr geschäftsführer, nunmehr|nun|jetzt xyz", by creating two records of those (one closing, one opening), Abberufen als Geschäftsführer und bestellt als Liquidator