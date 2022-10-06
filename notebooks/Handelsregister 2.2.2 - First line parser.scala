// Databricks notebook source
// MAGIC %md
// MAGIC # First Line Extractor
// MAGIC Each record starts with the name and address of the company in question. As we have many companies that have been founded before 2007 we will fetch the first record of each company and attempt to extract the name and address from there. This will build our base table. Later we will only look at specific mutations. Also we grab all other records to be able to check when we miss address changes etc.

// COMMAND ----------

// MAGIC %run ./includes/sentence_splitter

// COMMAND ----------

// MAGIC %md
// MAGIC # Base Table

// COMMAND ----------

import org.apache.spark.sql.functions._

val dfFirstRecords = spark.table("hreg_silver_publications_hrb_only")
.join(spark.table("hreg_gold_standardized_reference_numbers"), "referenceNumber")
.join(spark.table("hreg_gold_unifed_reference_numbers"), "stdRefNo")
.sort("hreg_silver_publications_hrb_only.dateOfPublication") // should be sorted this way already
.groupBy("stdRefNoUni")
.agg(first($"id").as("id"), first($"landAbk").as("landAbk"), first($"publicationBody").as("publicationBody"))

val dfFirstWithSentences = withSplitSentences(dfFirstRecords)

val dfPublications = withSplitSentences(spark.table("hreg_silver_publications_hrb_only"))


// COMMAND ----------

// MAGIC %md
// MAGIC # First Line Parser

// COMMAND ----------

import org.apache.spark.sql.functions._
import scala.util.matching.Regex
import org.apache.spark.sql.DataFrame


// important: to use needs to be escaped!
// todo: case insensitive match?
val stdCompanyTypes = Map[String, List[String]](
  "AG" -> List("AG", " ag", " aG", "Aktiengesellschaft", "a.G.", "AktiengeselIschaft", "Aktiengesellchaft", "a.G.", "a. G.", "A.G.", "Aktien Gesellschaft"),
  "eK" -> List("e.K.", "e. K.", "e. K", "eK"),
  "Inc" -> List("Inc."),
  "GmbH" -> List("GmbH", "GMBH", " mbH", " rnbH", "GmbH & Co", "Gesellschaft mit beschränkter Haftung", "GmbH - gemeinnützig", "gmbh", "Gmbh", "m.b.H.", "GESELLSCHAFT", "GmbH & Co", "GrnbH", "mbH & Co KG", "gbmh", "g.m.b.h.", "mit beschränkter Haftung", "Gesellschaft mit berschränkter Haftung", "mBH", "gmbH", "GmH", "GMbH", "Gesellschaft mit beschänkter Haftung", "G.M.B.H.", "GmtH", "mit beschänkter Haftung", "Gesellschaft mit beschrankter Haftung", "G.m.b.H", "G m b H", "mbh", "m.b.H", "m. b. H.", "Gesellschaft mit beschränker Haftung", "-Gesellschaft", "ges.mbH", "-mbH", " m b H"),
  "gGmbH" -> List("gemeinnützige Gesellschaft mit beschränkte Haftung", "GmbH - gemeinnützig -", "gGmbH", "gemeinnützige Gesellschaft mit beschränker Haftung"), 
  "Co. KG" -> List("Co. KG", "KG"),
  "Ltd" -> List("Ltd.", "LTD", "Limited", "LIMITED", "ltd.", " Ltd", " limited", "Ldt.", " LDT."),
  "OHG" -> List("OHG", "oHG"),
  /////////////"generic" -> List("gesellschaft". "Zweigniederlassung", "Niederlassung", " Inh."),
  "SE" -> List(" SE", "S.E.", "-SE"),
  "SA" -> List("S.A.", "Sociedad Anonima", "S. A.", "S. A.", " SA", "Société Anonyme", "S.A"),
  "Bank" -> List("Genossenschaftsbank"),
  "UG" -> List("(haftungsbeschränkt)", " UG", "UG (Haftungsbeschränkt)", "U G (Haftungsbeschränkt)", "UG (haftungsbeschänkt)", "haftungsbeschränkt", "UG (haftungbeschränkt)"),
  "gUG" -> List("gUG (haftungsbesschränkt)"),
  "sp zoo" -> List("sp.z o.o.", "Sp. z o.o.", "Spólka z o.o", "Sp. z.o.o", "SP. z o. o.", "sp. z o.o.", "sp.zoo", "Spólka z o.o", "Spolka z ograniczona odpowiedzialnoscia", "Gesellschaft mit begrenzter Haftung nach polnischem Recht", "SP.Z O.O.", "SPOLKA Z ORANICZONA ODPOWIEDZIALNOSCIA", " SPZOO", "Sp. zo.o"),
  "AS" -> List(" AS", " a. s.", "A/S", "A.S.", "a.s."),
  "APS" -> List(" ApS"),
  "Aktiebolag" -> List("Aktiebolag"),
  "SARL" -> List("s.à.r.l.", "S.À.R.L.", "Sàrl", "S.à r.l.", "s.a.r.l.", "S.a.r.l.", "S.à.r.l."),
  "SL" -> List("Sociedad Limitada", "S. L.", "SOCIEDAD LIMITADA", "limitata", "S.L.", " SL"),
  "eingetragener Kaufmannfrau" -> List("e.Kfr.", "e.Kfm.", "e. Kfm.", "e. Kfr."),
  "NV" -> List("N.V.", " NV", "N.V"),
  "SRO" -> List("s.r.o", "S.R.O."),
  "LLC" -> List("LLC", "L.L.C."),
  "DOO" -> List("D. O. O.", "D.O.O", " doo", " DOO", "D.O.O.", "d.o.o.", "d.o.o", "d. o. o."),
  "private unlimited company"  -> List("private unlimited company"),
  "EOOD" -> List(" EOOD"),
  "OOD" -> List(" OOD", " O.O.D."),
  "UAB" -> List(" UAB"),
  "AB" -> List(" AB"),
  "SRL" -> List("S.r.l.", "S. R. L.", "SRL", "S.R.L.", "S.R.L", "srl"),
  "NA" -> List("N.A.", " NA"),
  "BV" -> List("B.V.", " B. V.", " BV", " B.V"),
  "Vers. a. Gegegenseitigkeit" -> List("Krankenversicherung auf Gegenseitigkeit", "Lebensversicherung auf Gegenseitigkeit", "Versicherungsverein auf Gegenseitigkeit", "Kommunalversicherung auf Gegenseitigkeit", "VVaG", "V. VaG.", "Krankenversicherungsverein auf Gegenseitigkeit", "Verein auf Gegenseitigkeit", "Lebensversicherungsverein auf Gegenseitigkeit", "Versicherung auf Gegenseitigkeit", "Rückversicherungsverein auf Gegenseitigkeit"),
  "SPA" -> List("S.p.a", "S.p.A.", "S.P.A.", "S.p.A"),
  "EURL" -> List("EURL"),
  "SIA" -> List("SIA", " SIA"),
  "SAS" -> List(" SAS"),
  "PLC" -> List(" plc", " Plc", " PLC"),
  "Corporation" -> List("Corporation", "CORP.", "Corp."),
  "Inc" -> List("INC.", "Inc.", " Inc", "incorporated"),
  "LDA" -> List("LDA"),
  "SPRL" -> List("SPRL", "S.p.r.l"),
  "Kft" -> List("Korlatolt Felelössegü Tarsasag", "Korlátolt Felelösségü Társaság", "Kft.", " Kft", "KFT"),
  "Ügynöksèg" -> List("Ügynöksèg"),
  "KK" -> List("K. K. Japan", "K.K.", "K. K."),
  "OÜ" -> List(" OÜ"),
  "Gesellschaft auf Gegenseitigkeit" -> List("Gesellschaft auf Gegenseitigkeit"),
  "Bank" -> List("Landesbank", "Bank"),
  "CV" -> List("C.V."),
  "PV" -> List(" PV"),
  "ruGmbh" -> List("beschränkt haftende Gesellschaft nach russischem Recht")
)

val getCompanySuffixesRegex = () => {
  val r = stdCompanyTypes.values.map(s => s.map(Regex.quote).mkString("|")).mkString("|")
  s"($r)"
}

val companyTypes = getCompanySuffixesRegex()

def extractFirstLineInfo(df: DataFrame) = {
   df
  .withColumn("firstSentence", element_at($"sentences", 1))
  // if the firstSentence doesnt contain a postal code, extend it to the next sentence (this happens because we may split to aggressively in the sentence splitter, especially with Zweigniederlassungen, which is a stop word)
  .withColumn("firstSentence", when(
      $"firstSentence".rlike(".* \\d{5} .*") && !$"firstSentence".rlike("HRB .*\\d{5}.*:.*"), $"firstSentence").otherwise(concat_ws(" ", $"firstSentence", element_at($"sentences", 2))))
  .withColumn("name", regexp_extract($"firstSentence", s"^((.*?)$companyTypes(.*?)),", 1))
  .withColumn("zipAndPlaceAndRest", regexp_extract($"firstSentence", ", ?(\\d{5}.*)", 1))
  .withColumn("withoutCompanyName", expr("replace(firstSentence, name)"))
  .withColumn("assumedAddressWithPlace", expr("replace(withoutCompanyName, zipAndPlaceAndRest)"))
  .withColumn("assumedAddressWithPlaceSplit", split($"assumedAddressWithPlace", ",", 3))
  .withColumn("place", element_at($"assumedAddressWithPlaceSplit", 2))
  .withColumn("assumedAddress", element_at($"assumedAddressWithPlaceSplit", 3))
  .withColumn("assumedAddress", expr("nullif(trim(assumedAddress), '')"))
  .withColumn("zipAndPlaceSplit", split($"zipAndPlaceAndRest", "(\\(|\\)\\.)", 2))
  .withColumn("zipAndPlace", trim(element_at($"zipAndPlaceSplit", 1)))
  .withColumn("zipCode", regexp_extract($"zipAndPlace", ".*(\\d{5}).*", 1))
  .withColumn("restFirstLine", expr("replace(zipAndPlaceAndRest, zipAndPlace)"))
  .withColumn("restFirstLine", regexp_replace($"restFirstLine", "^(\\(|\\.| \\.|\\)\\.)", ""))
  .withColumn("lZipPlaceRest", length($"restFirstLine"))
  // if assumed address is still empty, check if we have format "place (address)"
  .withColumn("alternativeAssumedAddress", when($"place".rlike("^.*\\(.*[0-9]+.*$"), regexp_extract($"place", "^.*\\((.*[0-9]+.*)$", 1)))
  .withColumn("address", coalesce($"assumedAddress", $"alternativeAssumedAddress"))
  .withColumn("address", expr("REPLACE(address, '(', '')"))
  .withColumn("address", expr("REPLACE(address, ',', '')"))
  // if name extraction fails, just take the string before the first comma
  .withColumn("dirtyName", length($"name") === 0) // diryName implies that the know suffix finder was unsuccessful, we can look at firstLineRecords with diryName to find more company suffixes
  .withColumn("name", when(length($"name") === 0, element_at($"assumedAddressWithPlaceSplit", 1)).otherwise($"name"))
  .withColumn("name", regexp_replace($"name", "^HRB (.*):", ""))
  .withColumn("branchInfo", regexp_extract($"name", ".*(Zweigniederlassung|Branch).*", 1))
  .withColumn("name", regexp_replace($"name", "Zweigniederlassung .*", ""))
  .withColumn("name", expr("nullif(name, '')"))
  // edgecase: name is like Zweigniederlassung Deutschland EBFS AG Schweiz
  .withColumn("name", coalesce($"name", $"branchInfo"))
}

spark.sql("DROP TABLE IF EXISTS hreg_silver_company_first_line_stage")
extractFirstLineInfo(dfPublications)
.select($"globalId", $"firstSentence", $"referenceNumber", $"name", $"branchInfo", $"address", $"zipAndPlace", $"zipCode", $"dirtyName", $"dateOfPublication")
.write
.mode("overwrite")
.saveAsTable("hreg_silver_company_first_line_stage")

// COMMAND ----------

// MAGIC %md
// MAGIC # Finding more company suffixes

// COMMAND ----------

display(spark.table("hreg_silver_company_first_line_stage").where("dirtyName"))

// COMMAND ----------

// MAGIC %run ./includes/zip_place_cleaner

// COMMAND ----------


spark.sql("DROP TABLE IF EXISTS hreg_silver_company_first_line")
stripJunkFromZipAndPlace(spark.table("hreg_silver_company_first_line_stage")).write.mode("overwrite").saveAsTable("hreg_silver_company_first_line")

display(
  spark.table("hreg_silver_company_first_line")
)

// COMMAND ----------

// geocode with https://pypi.org/project/pgeocode/ ?

// http://download.geonames.org/export/zip/DE.zip

// COMMAND ----------

// MAGIC %md
// MAGIC # Events table, with the first line seperated
// MAGIC Here we build a table with addition of removing the first line from the publication body, to improve the performance of the sentence parser. This is possible because the first line ends on zip and place

// COMMAND ----------

import org.apache.spark.sql.functions._
import scala.util.matching.Regex
import org.apache.spark.sql.DataFrame


def removeFirstLineFromPublicationBody(dfBaseEvents: DataFrame, dfFirstLine: DataFrame) = {
  dfBaseEvents
  .join(dfFirstLine, "globalId")
  .withColumn("replaceRegex", concat(lit("^.*?"), lit("\\Q"), trim($"zipAndPlace"),  lit("\\E"), lit("\\)?\\. ?")))
  .withColumn("publicationBody", expr("regexp_replace(publicationBody, replaceRegex, '')"))
}

spark.sql("DROP TABLE IF EXISTS hreg_silver_events_hrb_only_first_line_cut")

removeFirstLineFromPublicationBody(spark.table("hreg_silver_publications_hrb_only"), spark.table("hreg_silver_company_first_line"))
.select("globalId", "hreg_silver_publications_hrb_only.referenceNumber", "typeOfPublication", "hreg_silver_publications_hrb_only.dateOfPublication", "publicationBody")
.write.saveAsTable("hreg_silver_events_hrb_only_first_line_cut")
display(spark.table("hreg_silver_events_hrb_only_first_line_cut"))

// COMMAND ----------

val dfStats = spark.table("hreg_silver_company_first_line")
.select($"name", $"branchInfo", $"address", $"zipAndPlace")
.withColumn("lengthName", length($"name"))
.withColumn("lengthBranchInfo", length($"branchInfo"))
.withColumn("lengthAddress", length($"address"))
.withColumn("lengthZipAndPlace", length($"zipAndPlace"))

display(
  dfStats
)