// Databricks notebook source
import org.apache.spark.sql.functions._

display(
  spark.table("hreg_people_stage1").withColumn("officerStr", explode($"officerStrList")).select("officerStrList", "peopleStr", "officerStr").orderBy("officerStr")
)

// COMMAND ----------

import org.apache.spark.sql.functions._

display(
  spark.table("hreg_people_stage1").withColumn("officerStr", explode($"officerStrList")).select("officerStrList", "peopleStr", "officerStr").groupBy("officerStr").count().orderBy(desc("count"))
)

// COMMAND ----------

import org.apache.spark.sql.functions._

display(
  spark.table("hreg_people_stage1")
  .withColumn("officerStr", explode($"officerStrList"))
  .select("officerStrList", "peopleStr", "officerStr", "officerStrList")
  .where("officerStr LIKE 'Selnes, Randi Mette, München, *02.03.1978'")
  /*.groupBy("officerStr")
  .agg(count("*"), max("peopleStr"), max("officerStrList"))
  .orderBy(asc("count(1)"))*/
)

// COMMAND ----------

import org.apache.spark.sql.functions._

display(
  spark.table("hreg_people_stage1").withColumn("officerStr", explode($"officerStrList")).select("officerStrList", "peopleStr", "officerStr").withColumn("lStr", length($"officerStr")).orderBy(desc("lStr"))
)

// COMMAND ----------

import org.apache.spark.sql.functions._

display(
  spark.table("hreg_people_stage1")
  .withColumn("officerStr", explode($"officerStrList"))
  .select("officerStrList", "peopleStr", "officerStr")
  .where("officerStr LIKE '%:%:%'")
  .groupBy("officerStr")
  .agg(count("*"), max("peopleStr"), max("officerStrList"))
  .orderBy(desc("count(1)"))
)

// COMMAND ----------

import org.apache.spark.sql.functions._

display(
  spark.table("hreg_people_stage1")
  .withColumn("officerStr", explode($"officerStrList"))
  .select("officerStrList", "peopleStr", "officerStr")
  .groupBy("officerStr")
  .agg(count("*"), max("peopleStr"), max("officerStrList"))
  .orderBy(desc("count(1)"))
  .where("officerStr LIKE ': %'")
)

// COMMAND ----------

import org.apache.spark.sql.functions._

display(
  spark.table("hreg_people_stage1")
  .withColumn("officerStr", explode($"officerStrList"))
  .select("officerStrList", "peopleStr", "officerStr")
  .groupBy("officerStr")
  .agg(count("*"), max("peopleStr"), max("officerStrList"))
  .orderBy(desc("count(1)"))
  .where("officerStr LIKE '%:%Prokura:%'")
)

// COMMAND ----------

import org.apache.spark.sql.functions._

display(
  spark.table("hreg_people_stage1")
  .withColumn("officerStr", explode($"officerStrList"))
  .select("officerStrList", "peopleStr", "officerStr")
  .where("peopleStr LIKE '%mit der Befugnis, im Namen der Gesellschaft mit sich als Vertreter der folgenden Gesellschaften Rechtsgeschäfte abzuschließen%'")
  .groupBy("officerStr")
  .agg(count("*"), max("peopleStr"), max("officerStrList"))
  .orderBy(desc("count(1)"))
)

// COMMAND ----------

import org.apache.spark.sql.functions._

display(
  spark.table("hreg_people_stage1")
  .withColumn("officerStr", explode($"officerStrList"))
  .select("officerStrList", "peopleStr", "officerStr")
  .groupBy("officerStr")
  .agg(count("*"), max("peopleStr"), max("officerStrList"))
  .orderBy(desc("count(1)"))
  .where("officerStr LIKE 'Gesamtprokura%' AND officerStr NOT LIKE '%*%'")
)

// COMMAND ----------



// COMMAND ----------

import org.apache.spark.sql.functions._

display(
  spark.table("hreg_people_stage1")
  .withColumn("officerStr", explode($"officerStrList"))
  .select("officerStrList", "peopleStr", "officerStr")
  .groupBy("officerStr")
  .agg(count("*"), max("peopleStr"), max("officerStrList"))
  .orderBy(desc("count(1)"))
  .where("officerStr LIKE '%*%*%'")
)

// COMMAND ----------

import org.apache.spark.sql.functions._

val dfCleaned = spark.table("hreg_people_stage0").withColumn("officerStr", explode($"officerStrList")).where("officerStr LIKE '%:%:%'")

display(dfCleaned.select("id", "peopleStr", "officerStr", "publicationBody", "officerStrList").where("officerStr LIKE '%:%:%'").orderBy("officerStr"))

// COMMAND ----------

// MAGIC %sql
// MAGIC WITH q AS (
// MAGIC   SELECT explode(extractedOfficers) AS officer FROM hreg_people_stage1
// MAGIC ), q1 AS (
// MAGIC   SELECT officer.position, count(*) as cnt, max(officer.inputString) FROM q WHERE officer.position NOT LIKE '%Geschäftsführer%' GROUP BY 1 ORDER BY 2
// MAGIC  )
// MAGIC  SELECT * FROM q1
// MAGIC  --SELECT count(*) FROM q1 WHERE cnt < 3

// COMMAND ----------

// MAGIC %sql
// MAGIC WITH q AS (
// MAGIC   SELECT id, explode(officerStrList) as os FROM hreg_people_stage1
// MAGIC ), q2 AS (
// MAGIC SELECT id, os, extractOfficerOrCompany(os) e FROM q --extractOneOfficer(os)
// MAGIC )
// MAGIC SELECT (SELECT count(*) FROM q2 WHERE e.position = "PERSON_EXTRACTION_ERROR")/count(*) AS error_rate FROM q2 WHERE e.position != "PERSON_EXTRACTION_ERROR"
// MAGIC --ORDER BY RANDOM()

// COMMAND ----------

import org.apache.spark.sql.functions._

display(
  spark.table("hreg_people_stage1")
  .select($"globalId", posexplode($"extractedOfficers"))
  .select("globalId", "pos", "col.*")
  .withColumn("formerSplit", split($"position", "(?<=(Nicht mehr|War|war |Ausgeschieden als|Ausgeschieden|nicht mehr))", 2))
  .withColumn("position", trim(element_at($"formerSplit", -1)))
  .withColumn("former", size($"formerSplit") === 2)
  .withColumn("appointedSplit", split($"position", "(?<=(Bestellt zum|Bestellt als|Bestellt |Geschäftsführer nunmehr bestellt als|Geschäftsführer, jetzt bestellt als|Nun bestellt als))", 2))
  .withColumn("position", trim(element_at($"appointedSplit", -1)))
  .withColumn("appointed", size($"appointedSplit") === 2)
  .groupBy("position")
  .agg(count("*"), max("inputString"))
  .orderBy(desc("count(1)"))
)

// COMMAND ----------

// MAGIC %sql
// MAGIC WITH q AS (
// MAGIC   SELECT id, landAbk, explode(extractedOfficers) as os FROM hr_stage2
// MAGIC )
// MAGIC SELECT * FROM q WHERE os.city LIKE '%/%' OR os.city LIKE '%)%' OR os.city LIKE '%,%'

// COMMAND ----------

// MAGIC %sql
// MAGIC WITH q AS (
// MAGIC   SELECT id, landAbk, explode(extractedOfficers) as os FROM hr_stage2
// MAGIC )
// MAGIC SELECT * FROM q ORDER BY length(os.lastName) DESC ---os.city LIKE '%/%' OR os.city LIKE '%)%' OR os.city LIKE '%,%'

// COMMAND ----------

// MAGIC %sql
// MAGIC WITH q AS (
// MAGIC   SELECT id, landAbk, explode(extractedOfficers) as os FROM hr_stage2
// MAGIC )
// MAGIC SELECT id, landAbk, length(os.lastName) FROM q ORDER BY length(os.lastName) DESC ---os.city LIKE '%/%' OR os.city LIKE '%)%' OR os.city LIKE '%,%'

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM hr_stage2 WHERE id = 182367 AND landAbk = 'be'

// COMMAND ----------

// MAGIC %sql
// MAGIC WITH q AS (
// MAGIC   SELECT id, landAbk, explode(officerStrList) as os FROM hr_stage2
// MAGIC )
// MAGIC SELECT * FROM q WHERE os = 'Einzelprokura' --GROUP BY 1

// COMMAND ----------



// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT id, landAbk, typeOfPublication, publicationBody, fullText, peopleStr, officerStrList, extractedOfficers
// MAGIC FROM hr_stage2 WHERE publicationBody LIKE '%Prokura geändert%'

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE hr_officers AS (
// MAGIC WITH q AS (
// MAGIC   SELECT 
// MAGIC     *,
// MAGIC     posexplode(extractedOfficers) AS (pos, officer)
// MAGIC   FROM hr_stage2
// MAGIC ), q2 AS (
// MAGIC   SELECT 
// MAGIC     --court,
// MAGIC     id,
// MAGIC     landAbk,
// MAGIC     pos,
// MAGIC     NULLIF(officer.position, '') as role, -- todo refactor overall to role, null before here
// MAGIC     officer.position as roleFilled, -- for backwards comp, refactor
// MAGIC     officer.positionQualifier as positionQualifier,
// MAGIC     officer.lastName as lastName,
// MAGIC     officer.firstName as firstName,
// MAGIC     SPLIT(officer.city, ",") as cityParts,
// MAGIC     officer.birthDate as birthDate,
// MAGIC     officer.court as court,
// MAGIC     officer.internalNumber as internalNumber,
// MAGIC     officer.isCompany as isCompany,
// MAGIC     officer.inputString as inputString,
// MAGIC     officer.remainder AS remainder,
// MAGIC     dateOfPublication
// MAGIC   FROM q
// MAGIC )
// MAGIC SELECT 
// MAGIC   id,
// MAGIC   landAbk,
// MAGIC   pos,
// MAGIC   role,
// MAGIC   lastName,
// MAGIC   firstName,
// MAGIC   CASE size(cityParts) 
// MAGIC     WHEN 2 THEN element_at(cityParts, 2)
// MAGIC     ELSE element_at(cityParts, 1)
// MAGIC   END as city,
// MAGIC   CASE size(cityParts) 
// MAGIC     WHEN 2 THEN element_at(cityParts, 1)
// MAGIC   END as cityExtra,
// MAGIC   birthDate,
// MAGIC   court,
// MAGIC   internalNumber,
// MAGIC   positionQualifier,
// MAGIC   isCompany,
// MAGIC   inputString,
// MAGIC   remainder,
// MAGIC   'TODO' AS country, -- todo
// MAGIC   to_date(dateOfPublication, "dd.MM.yyyy") AS dateOfPublication
// MAGIC FROM q2
// MAGIC )
// MAGIC /*SELECT
// MAGIC   lbk,
// MAGIC   sum(cnt)
// MAGIC FROM cnts
// MAGIC WHERE cnt = 1
// MAGIC GROUP BY 1*/
// MAGIC 
// MAGIC --WHERE name LIKE "%geb%"

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val dfD = spark.table("hr_officers")
val window = Window
    .partitionBy("id", "landAbk")
    .orderBy("pos")
    .rowsBetween(-100, 0)

spark.sql("DROP TABLE IF EXISTS hr_filled;")

val dfF = dfD
.withColumn("roleFilled", last("role", true).over(window)) // check if it works for multiple roles in same statement (eg Nicht mehr geschäftsführer: bla, Geschäftsführerer: bla; bla; Prouka: bla) eg 403065, be
.withColumn("roleFilledLower", lower($"roleFilled"))
.withColumn("former", expr("roleFilledLower LIKE '%war%' OR roleFilledLower LIKE '%nicht mehr%' OR roleFilledLower LIKE '%ausgeschieden%' OR roleFilledLower LIKE '%erloschen%' OR roleFilledLower LIKE '%als nicht eingetragen%'"))
.withColumn("appointed", expr("roleFilledLower LIKE '%bestellt%'"))
.withColumn("positionId", expr("row_number() OVER (ORDER BY id, landAbk, firstName, lastName, birthDate)"))
.withColumn("startDate", when(!col("former"), col("dateOfPublication")).otherwise(null))
.withColumn("endDate", when(col("former"), col("dateOfPublication")).otherwise(null))

dfF.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("hr_filled")

//val dfBrokenRole = dfF
//.where("roleFilled NOT LIKE '%*%'")

val dfOK = dfF.where("roleFilled NOT LIKE '%*%' AND NOT (appointed AND former)") // exclude bestellt und ausgeschieden as it is kind of a noop

val dfPeople = dfOK.where(dfOK("isCompany") === false).createOrReplaceTempView("hr_officers_people")
val dfCompanyOwners = dfOK.where(dfOK("isCompany") === true).drop("lastName").withColumnRenamed("firstName", "entityName").withColumn("entityName", regexp_replace(col("entityName"), ";", "")).createOrReplaceTempView("hr_officers_companies")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM hr_officers ORDER BY RANDOM()

// COMMAND ----------

import org.apache.spark.sql.functions._

def calculateZScoresForDf(dfF: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
  val keepCols = Seq("id", "landAbk", "positionId", "pos")
  val keepColsSpark = keepCols.map(c => col(c))
  val selectCols = dfF.columns.filter(c => !keepCols.contains(c.toString))
  val dfLenghts = dfF.select(keepColsSpark ++ selectCols.map(c => length(col(c)).alias(c)): _*)
  val dfAvgLengths = dfLenghts.select(selectCols.map(c => avg(col(c)).alias(s"${c}_avg")): _*)
  val dfStdevLengths = dfLenghts.select(selectCols.map(c => stddev_pop(col(c)).alias(s"${c}_std")): _*)

  val selectColsStr = selectCols.map(c => s"${c.toString}_z_score").mkString(",")

  val dfZScores = dfLenghts
                    .join(dfAvgLengths)
                    .join(dfStdevLengths)
                    .select(keepColsSpark ++ selectCols.map(c => ((col(c) - col(s"${c}_avg")) / col(s"${c}_std")).alias(s"${c}_z_score")): _*)
                    .withColumn("max_z", expr(s"greatest(${selectColsStr})"))

  return dfZScores  
}


// COMMAND ----------

import org.apache.spark.sql.functions._


val stage3People = spark.sql("""
WITH former_roles_cleaned AS (
    SELECT
        id,
        landAbk,
        positionId,
        trim(regexp_replace(roleFilled, '(?i)(Prokura erloschen : War|Nicht eingetragen, nicht mehr|nicht mehr(:)?|Bestellt und nicht mehr|war|Ausgeschieden:|Ausgeschieden als|Ausgeschieden|erloschen|Vorstand: War|
Prokura: War|Bestellt und (sodann|später) wieder ausgeschieden: |Der hier nicht eingetragen gewesene ist nicht mehr|Bestellt(.*)ausgeschieden (als)?)|Nicht eingetragene und inzwischen erloschene',
                            '')) AS roleFilledClean
    FROM hr_officers_people
    WHERE former AND NOT appointed
),
    current_roles_cleaned AS (
        SELECT
            id,
            landAbk,
            positionId,
            CASE
                WHEN roleFilled LIKE 'Einzelprokura%'
                    THEN 'Einzelprokura'
                WHEN roleFilled LIKE 'Gesamtprokura%'
                    THEN 'Gesamtprokura'
                WHEN roleFilled LIKE 'Prokura%'
                    THEN 'Prokura'
                WHEN roleFilled LIKE 'Inhaber%'
                    THEN 'Inhaber'
                WHEN roleFilledLower RLIKE 'geschäftsführer(.*)' THEN 'Geschäftsführer'
                WHEN roleFilled LIKE 'director'
                    THEN 'Director'
                WHEN roleFilled LIKE 'direktor'
                    THEN 'Director'
                WHEN roleFilled LIKE 'Direktorin'
                    THEN 'Director'
                WHEN roleFilledLower RLIKE 'persönlich haftende gesellschafterin' THEN 'persönlich haftender Gesellschafter'
                WHEN roleFilled LIKE 'Eingetreten als Persönlich haftender Gesellschafter'
                    THEN 'persönlich haftender Gesellschafter'
                WHEN roleFilled LIKE 'Eingetreten: Persönlich haftender Gesellschafter'
                    THEN 'persönlich haftender Gesellschafter'
                WHEN roleFilled LIKE 'Partner: Partner'
                    THEN 'Partner'
                WHEN roleFilled RLIKE 'Vorstand: (.*)' THEN 'Vorstand'
                WHEN roleFilled LIKE 'Vorstandsmitglied'
                    THEN 'Vorstand'
                WHEN roleFilled LIKE 'Erteilt: Einzelprokura'
                    THEN 'Einzelprokura'
                ELSE roleFilled
                END AS roleFilledClean
        FROM hr_officers_people
        WHERE NOT former AND NOT appointed
    ),
    appointed_roles_cleaned AS (
        SELECT
            id,
            landAbk,
            positionId,
            trim(regexp_replace(roleFilled,
                                '(?i)((Neu )?Bestellt(.*)(als|zum)?:|Bestellt (zum|zur)|Bestellt und(sodann )? wieder ausgeschieden als|Bestellt(.*)ausgeschieden(:)?|(Neu )?Bestellt(.*)als|Bestellt und wieder abberufen |Neu bestellt zum weiteren|Bestellt:|Bestellt) ',
                                '')) AS roleFilledClean
        FROM hr_officers_people
        WHERE appointed
    )
SELECT
    p.*,
    coalesce(fc.roleFilledClean, cc.roleFilledClean, ac.roleFilledClean) AS roleFilledClean
FROM hr_officers_people p
LEFT JOIN former_roles_cleaned fc USING (id, landAbk, positionId)
LEFT JOIN current_roles_cleaned cc USING (id, landAbk, positionId)
LEFT JOIN appointed_roles_cleaned ac USING (id, landAbk, positionId)
""").drop("role", "roleFilled", "roleFilledLower")



val dfFullStage3 = stage3People
.withColumn("lastName", regexp_replace($"lastName", "(Entstanden|Die Gesellschaft)(.*)(Einzelkaufmann|Einzelkauffrau)\\s?", ""))
.withColumn("lastName", regexp_replace($"lastName", "Die Firma des bisher nicht eingetragenen Inhabers ", ""))
.withColumn("city", regexp_replace($"city", "mit der Befugnis(.*)", "")) // trailing mit der befugniss in city
val stage3PeopleZScores = calculateZScoresForDf(dfFullStage3)

dfFullStage3
//.join(stage3PeopleZScores, "positionId")
.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_stage_3_people")



val stage3Companies = spark.sql("""
SELECT
  c.*,
  CASE 
    WHEN roleFilled LIKE '%Inhaber:%' THEN 'Inhaber'
    WHEN roleFilledLower RLIKE 'persönlich haftende(r)? gesellschafter(in)?' THEN 'Persönlich haftender Gesellschafter'
    WHEN roleFilledLower RLIKE 'liquidator' THEN 'Liquidator'
    ELSE roleFilled
  END as roleFilledClean
FROM hr_officers_companies c
""")

val stage3CompanyZScores = calculateZScoresForDf(stage3Companies)

stage3Companies
//.join(stage3CompanyZScores, "positionId")
.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("hr_stage_3_companies")
//.cache()



// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE hr_stage_3_companies;
// MAGIC DROP TABLE hr_stage_3_people;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM hr_stage2 WHERE referenceNumber = 'Frankfurt am Main HRB 40596'

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE test_case_fra AS (
// MAGIC SELECT * FROM hr_stage_3_people
// MAGIC LEFT JOIN (SELECT id, landAbk, referenceNumber FROM hr_stage2) USING (id, landAbk)
// MAGIC WHERE referenceNumber = 'Frankfurt am Main HRB 40596'
// MAGIC ORDER BY to_date(dateOfPublication, "dd.MM.yyyy"), pos
// MAGIC );
// MAGIC SELECT * FROM test_case_fra 

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT firstName, lastName, count(*), max(birthDate), max(former), min(startDate), max(endDate)
// MAGIC FROM test_case_fra
// MAGIC GROUP BY 1, 2
// MAGIC ORDER BY 2

// COMMAND ----------

// MAGIC %sql
// MAGIC WITH q AS (
// MAGIC   SELECT lastName, firstName, city, birthDate,  roleFilledClean, former, appointed, startDate, lead(endDate) OVER (PARTITION BY lastName, firstName ORDER BY dateOfPublication, former DESC) as endDate
// MAGIC   FROM hr_stage_3_people
// MAGIC   LEFT JOIN (SELECT id, landAbk, referenceNumber FROM hr_stage2) USING (id, landAbk)
// MAGIC   --WHERE lastName='Fries'
// MAGIC   ORDER BY dateOfPublication, former
// MAGIC )
// MAGIC SELECT * FROM q WHERE NOT former
// MAGIC ORDER BY lastName

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT concat(firstName, ' ', lastName) FROM hr_stage_3_people

// COMMAND ----------

THIS ONLY PULLS started positions, not ones that predate the current bekanntmachungen.
also check for bugs in roles (special characters , and .) and other fields also still many person extraction errors

https://stackoverflow.com/questions/38872592/how-to-filter-data-using-window-functions-in-spark

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE roles_people AS (
// MAGIC   WITH q AS (
// MAGIC     SELECT positionId, stdRefNoUni, id AS publicationId, landAbk AS publicationState, lastName, firstName, birthDate, roleFilledClean, former, appointed, inputString, city, country, internalNumber, positionQualifier, startDate, lead(endDate) OVER (PARTITION BY stdRefNoUni, lastName, firstName ORDER BY dateOfPublication, former DESC) as endDate, lead(positionId) OVER (PARTITION BY referenceNumber, lastName, firstName ORDER BY dateOfPublication, former DESC) as endPosition
// MAGIC     FROM hr_stage_3_people
// MAGIC     LEFT JOIN (SELECT id, landAbk, stdRefNoUni FROM hr_stage2) USING (id, landAbk)
// MAGIC     ORDER BY dateOfPublication, former
// MAGIC   ),
// MAGIC   started_roles AS (
// MAGIC     SELECT * FROM q WHERE NOT former
// MAGIC     ORDER BY startDate, lastName
// MAGIC   ),
// MAGIC   open_start_roles AS ( --positions w/o start date
// MAGIC     SELECT * FROM q WHERE former AND positionId NOT IN (SELECT endPosition FROM q)
// MAGIC   )
// MAGIC   SELECT * FROM started_roles
// MAGIC   WHERE roleFilledClean != 'PERSON_EXTRACTION_ERROR' 
// MAGIC   --UNION --broken
// MAGIC   --SELECT * FROM open_start_roles
// MAGIC );
// MAGIC 
// MAGIC CREATE OR REPLACE TABLE roles_companies AS (
// MAGIC   WITH q AS (
// MAGIC     SELECT positionId, stdRefNoUni, id AS publicationId, landAbk AS publicationState, entityName, city, birthDate, roleFilledClean, former, appointed, inputString, country, court, startDate, lead(endDate) OVER (PARTITION BY stdRefNoUni, entityName ORDER BY dateOfPublication, former DESC) as endDate
// MAGIC     FROM hr_stage_3_companies
// MAGIC     LEFT JOIN (SELECT id, landAbk, stdRefNoUni FROM hr_stage2) USING (id, landAbk)
// MAGIC     ORDER BY dateOfPublication, former
// MAGIC   )
// MAGIC   SELECT * FROM q 
// MAGIC   WHERE NOT former AND roleFilledClean != 'PERSON_EXTRACTION_ERROR' 
// MAGIC   ORDER BY startDate, entityName
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM hr_stage2
// MAGIC --LEFT JOIN (SELECT id, landAbk, referenceNumber FROM hr_stage2) USING (id, landAbk)
// MAGIC WHERE referenceNumber = 'Schweinfurt HRB 4704' --AND lastName = 'Vierkötter'

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM hr_stage_3_people
// MAGIC LEFT JOIN (SELECT id, landAbk, referenceNumber FROM hr_stage2) USING (id, landAbk)
// MAGIC WHERE referenceNumber = 'Charlottenburg (Berlin) HRB 134456 B'
// MAGIC ORDER BY to_date(dateOfPublication, "dd.MM.yyyy")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM roles_people
// MAGIC WHERE referenceNumber = 'Charlottenburg (Berlin) HRB 134456 B'

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM hr_stage2 WHERE referenceNumber = 'Charlottenburg (Berlin) HRB 134456 B' ORDER BY id

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM hr_stage_3_people
// MAGIC LEFT JOIN (SELECT id, landAbk, referenceNumber FROM hr_stage2) USING (id, landAbk)
// MAGIC WHERE referenceNumber = 'Frankfurt am Main HRB 40596'
// MAGIC ORDER BY to_date(dateOfPublication, "dd.MM.yyyy")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT referenceNumber, count(*) FROM hr_stage_3_people
// MAGIC LEFT JOIN (SELECT id, landAbk, referenceNumber FROM hr_stage2) USING (id, landAbk)
// MAGIC GROUP BY 1 ORDER BY 2 DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT lastName, count(*) FROM persons GROUP BY 1 ORDER BY 2 DESC