// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

// COMMAND ----------

// tables
spark.table("hreg_standardized_reference_numbers")
spark.table("hreg_gold_unified_reference_numbers")
spark.table("hreg_gold_companies_positions")
spark.table("hreg_gold_company_dates")
spark.table("hreg_gold_company_objectives")
spark.table("hreg_gold_company_address")
spark.table("hreg_gold_company_names")
spark.table("hreg_gold_company_capital")

// COMMAND ----------

display(spark.table("hreg_gold_unified_reference_numbers"))

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

spark.sql("DROP TABLE IF EXISTS hreg_star_dim_reference_numbers")

val dfDimReferenceNumbers = spark.table("hreg_standardized_reference_numbers").join(spark.table("hreg_unifed_reference_numbers"), "stdRefNo")
.withColumn("validTill", lead($"referenceNumberFirstSeen", 1).over(Window.partitionBy("stdRefNoUni").orderBy($"referenceNumberFirstSeen")))
.withColumn("isCurrent", $"validTill".isNull)
.withColumn("courtCode", expr("courtMatch.code"))
.withColumn("courtName", expr("courtMatch.court"))
.select($"stdRefNoUni".alias("companyId"), $"stdRefNo", $"referenceNumber".alias("nativeReferenceNumber"), $"courtName", $"courtCode", $"referenceNumberFirstSeen", $"validTill")


dfDimReferenceNumbers.write.mode("overwrite").saveAsTable("hreg_star_dim_reference_numbers")

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS hreg_star_fact_companies")
val dfCenter = spark.table("hreg_gold_unified_reference_numbers").select("stdRefNoUni").join(spark.table("hreg_gold_company_dates"), "stdRefNoUni", "outer").withColumnRenamed("stdRefNoUni", "companyId")

dfCenter.write.mode("overwrite").saveAsTable("hreg_star_fact_companies")

assert (dfCenter.count() == dfCenter.select(countDistinct($"companyId").alias("cntD")).collect()(0).getAs[Long]("cntD")) 

// COMMAND ----------



// COMMAND ----------


val dfPositions = spark.table("hreg_gold_companies_positions").withColumnRenamed("stdRefNoUni", "companyId")
dfPositions.write.mode("overwrite").saveAsTable("hreg_star_dim_positions")

display(spark.table("hreg_star_dim_positions"))

// COMMAND ----------

import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

spark.sql("DROP TABLE IF EXISTS hreg_star_dim_objectives")

val dfObjectives = spark.table("hreg_gold_company_objectives")
  .withColumnRenamed("stdRefNoUni", "companyId")
  .withColumnRenamed("dateOfPublication", "validFrom")
  .withColumn("validTill", lead($"validFrom", 1).over(Window.partitionBy("companyId").orderBy($"validFrom")))
  .withColumn("isCurrent", $"validTill".isNull)

dfObjectives.write.mode("overwrite").saveAsTable("hreg_star_dim_objectives")

// COMMAND ----------

spark.sql("DROP TABLE IF EXISTS hreg_star_company_address")

val dfAddresses = spark.table("hreg_gold_company_address")
  .withColumnRenamed("stdRefNoUni", "companyId")
  .withColumnRenamed("dateOfPublication", "validFrom")
  .withColumn("validTill", lead($"validFrom", 1).over(Window.partitionBy("companyId").orderBy($"validFrom")))
  .withColumn("isCurrent", $"validTill".isNull)

dfAddresses.write.mode("overwrite").saveAsTable("hreg_star_dim_addresses")

display(
  dfAddresses
)

// COMMAND ----------



spark.sql("DROP TABLE IF EXISTS hreg_star_dim_names")

val dfNames = spark.table("hreg_gold_company_names")
  .withColumnRenamed("stdRefNoUni", "companyId")
  .withColumnRenamed("dateOfPublication", "validFrom")
  .withColumn("validTill", lead($"validFrom", 1).over(Window.partitionBy("companyId").orderBy($"validFrom")))
  .withColumn("isCurrent", $"validTill".isNull)

dfNames.write.mode("overwrite").saveAsTable("hreg_star_dim_names")

display(
  dfNames
)

// COMMAND ----------



spark.sql("DROP TABLE IF EXISTS hreg_star_company_capital")

val dfCapital = spark.table("hreg_gold_company_capital")
  .withColumnRenamed("stdRefNoUni", "companyId")
  .withColumnRenamed("dateOfPublication", "validFrom")
  .withColumn("validTill", lead($"validFrom", 1).over(Window.partitionBy("companyId").orderBy($"validFrom")))
  .withColumn("isCurrent", $"validTill".isNull)

dfCapital.write.mode("overwrite").saveAsTable("hreg_star_dim_capital")

display(
  dfCapital
)

// COMMAND ----------

# https://app.quickdatabasediagrams.com/#/
# Modify this code to update the DB schema diagram.
# To reset the sample schema, replace everything with
# two dots ('..' - without quotes).

Companies
-
companyId PK varchar
firstSeenDate date
lastSeenDate date
foundedDate date
dissolutionDate date

Addresses
-
companyId varchar FK >- companies.companyId
globalId varchar
validFrom date
fullAddress varchar
address varchar
zipCode varchar
zipAndPlace varchar
validTill date
isCurrent boolean

Capital
----
companyId varchar FK >- companies.companyId
globalId varchar
validFrom date
objective varchar
validTill date
isCurrent boolean

Names
----
companyId varchar FK >- companies.companyId
globalId varchar
validFrom date
name varchar
validTill date
isCurrent boolean

Objectives
----
companyId varchar FK >- companies.companyId
globalId varchar
validFrom date
objective varchar
validTill date
isCurrent boolean

Positions
----
companyId varchar FK >- companies.companyId
globalId varchar
dateOfPublication date
foundPosition varchar
startDate date
endDate date
firstName varchar
lastName varchar
maidenName varchar
birthDate varchar
city varchar

ReferenceNumbers
----
companyId varchar FK >- companies.companyId
stdRefNo varchar
nativeReferenceNumber varchar
courtName varchar
courtCode varchar
referenceNumberFirstSeen date
validTill date

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC import sqlite3
// MAGIC import pyspark.pandas as ps
// MAGIC import os
// MAGIC 
// MAGIC schema = """
// MAGIC CREATE TABLE "Companies" (
// MAGIC     "companyId" varchar,
// MAGIC     "firstSeenDate" date,
// MAGIC     "lastSeenDate" date,
// MAGIC     "foundedDate" date,
// MAGIC     "dissolutionDate" date
// MAGIC );
// MAGIC 
// MAGIC CREATE TABLE "Addresses" (
// MAGIC     "companyId" varchar,
// MAGIC     "globalId" varchar,
// MAGIC     "validFrom" date,
// MAGIC     "fullAddress" varchar,
// MAGIC     "address" varchar,
// MAGIC     "zipCode" varchar,
// MAGIC     "zipAndPlace" varchar,
// MAGIC     "validTill" date,
// MAGIC     "isCurrent" boolean
// MAGIC );
// MAGIC 
// MAGIC CREATE TABLE "Capital" (
// MAGIC     "companyId" varchar,
// MAGIC     "globalId" varchar,
// MAGIC     "validFrom" date,
// MAGIC     "objective" varchar,
// MAGIC     "validTill" date,
// MAGIC     "isCurrent" boolean
// MAGIC );
// MAGIC 
// MAGIC CREATE TABLE "Names" (
// MAGIC     "companyId" varchar,
// MAGIC     "globalId" varchar,
// MAGIC     "validFrom" date,
// MAGIC     "name" varchar,
// MAGIC     "validTill" date,
// MAGIC     "isCurrent" boolean
// MAGIC );
// MAGIC 
// MAGIC CREATE TABLE "Objectives" (
// MAGIC     "companyId" varchar,
// MAGIC     "globalId" varchar,
// MAGIC     "validFrom" date,
// MAGIC     "objective" varchar,
// MAGIC     "validTill" date,
// MAGIC     "isCurrent" boolean
// MAGIC );
// MAGIC 
// MAGIC CREATE TABLE "Positions" (
// MAGIC     "companyId" varchar,
// MAGIC     "globalId" varchar,
// MAGIC     "dateOfPublication" date,
// MAGIC     "foundPosition" varchar,
// MAGIC     "startDate" date,
// MAGIC     "endDate" date,
// MAGIC     "firstName" varchar,
// MAGIC     "lastName" varchar,
// MAGIC     "maidenName" varchar,
// MAGIC     "birthDate" varchar,
// MAGIC     "city" varchar
// MAGIC );
// MAGIC 
// MAGIC CREATE TABLE "ReferenceNumbers" (
// MAGIC     "companyId" varchar,
// MAGIC     "stdRefNo" varchar,
// MAGIC     "nativeReferenceNumber" varchar,
// MAGIC     "courtName" varchar,
// MAGIC     "courtCode" varchar,
// MAGIC     "referenceNumberFirstSeen" date,
// MAGIC     "validTill" date
// MAGIC );
// MAGIC 
// MAGIC """
// MAGIC 
// MAGIC try:
// MAGIC   os.remove("export.db")
// MAGIC except FileNotFoundError:
// MAGIC   pass
// MAGIC 
// MAGIC con = sqlite3.connect('export.db')
// MAGIC cur = con.cursor()
// MAGIC for createStmt in schema.split(";"):
// MAGIC   cur.execute(createStmt)
// MAGIC   con.commit()
// MAGIC con.close()

// COMMAND ----------

// MAGIC %python
// MAGIC spark.table("hreg_star_fact_companies").toPandas().to_csv("companies.csv", index=False)
// MAGIC spark.table("hreg_star_dim_addresses").toPandas().to_csv("addresses.csv", index=False)
// MAGIC spark.table("hreg_star_dim_capital").toPandas().to_csv("capital.csv", index=False)
// MAGIC spark.table("hreg_star_dim_objectives").toPandas().to_csv("objectives.csv", index=False)
// MAGIC spark.table("hreg_star_dim_reference_numbers").toPandas().to_csv("referenceNumbers.csv", index=False)
// MAGIC spark.table("hreg_star_dim_names").toPandas().to_csv("names.csv", index=False)
// MAGIC spark.table("hreg_star_dim_positions").toPandas().to_csv("positions.csv", index=False)

// COMMAND ----------

// MAGIC %sh
// MAGIC apt-get update && apt-get install sqlite3 -yq
// MAGIC sqlite3 -separator ',' export.db ".import companies.csv Companies"
// MAGIC sqlite3 -separator ',' export.db ".import names.csv Names"
// MAGIC sqlite3 -separator ',' export.db ".import objectives.csv Objectives"
// MAGIC sqlite3 -separator ',' export.db ".import positions.csv Positions"
// MAGIC sqlite3 -separator ',' export.db ".import referenceNumbers.csv ReferenceNumbers"
// MAGIC sqlite3 -separator ',' export.db ".import addresses.csv Addresses"

// COMMAND ----------

// MAGIC %python
// MAGIC import sqlite3
// MAGIC 
// MAGIC indexes = """
// MAGIC CREATE INDEX "addressCompanyId" ON "Addresses" (
// MAGIC 	"companyId"
// MAGIC );
// MAGIC 
// MAGIC CREATE INDEX "CompaniesCompanyId" ON "Companies" (
// MAGIC 	"companyId"
// MAGIC );
// MAGIC 
// MAGIC CREATE INDEX "NamesCompanyId" ON "Names" (
// MAGIC 	"companyId"
// MAGIC );
// MAGIC 
// MAGIC CREATE INDEX "ObjectivesCompanyId" ON "Objectives" (
// MAGIC 	"companyId"
// MAGIC );
// MAGIC 
// MAGIC CREATE INDEX "PositionsCompanyId" ON "Positions" (
// MAGIC 	"companyId"
// MAGIC );
// MAGIC 
// MAGIC CREATE INDEX "RefNocompanyId" ON "ReferenceNumbers" (
// MAGIC 	"companyId"
// MAGIC );
// MAGIC 
// MAGIC CREATE VIRTUAL TABLE NamesFts USING fts5(companyId, name);
// MAGIC 
// MAGIC CREATE VIRTUAL TABLE ObjectivesFts USING fts5(companyId, name);
// MAGIC 
// MAGIC INSERT INTO NamesFts SELECT companyId, name FROM Names;
// MAGIC INSERT INTO ObjectivesFts SELECT companyId, objective FROM Objectives;
// MAGIC 
// MAGIC """
// MAGIC 
// MAGIC con = sqlite3.connect('export.db')
// MAGIC cur = con.cursor()
// MAGIC for createStmt in indexes.split(";"):
// MAGIC   cur.execute(createStmt)
// MAGIC   con.commit()
// MAGIC con.close()

// COMMAND ----------

// MAGIC %sh
// MAGIC ls -lh

// COMMAND ----------

// MAGIC %sh
// MAGIC #bzip2 export.db
// MAGIC cp export.db /dbfs/FileStore/

// COMMAND ----------

// use to json to serialize rows https://spark.apache.org/docs/latest/api/sql/index.html#to_json, collect with collect list
val df = spark.table("hreg_star_dim_addresses")
val cols = df.columns.toSeq

//spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

val aggColumnsToList = (df:DataFrame) => {
  val cols = df.drop("companyId").columns.toSeq.map(c => s"'$c', $c").mkString(",")
  val c = s"to_json(named_struct($cols))"
  df
    .withColumn("json", expr(c))
    .withColumn("obj", from_json($"json", df.drop("companyId").schema))
    .groupBy("companyId")
    .agg(
      collect_list($"obj").alias("list")
    )
}

val dfCompanies = spark.table("hreg_star_fact_companies")//.drop("firstSeenDate", "lastSeenDate", "foundedDate", "dissolutionDate")
val dfLatestName = spark.table("hreg_star_dim_names").where("isCurrent").select($"companyId", $"name".alias("currentName"))
val dfLatestObjective = spark.table("hreg_star_dim_objectives").where("isCurrent").select($"companyId", $"objective".alias("currentObjective"))
val dfLatestAddress = spark.table("hreg_star_dim_addresses").where("isCurrent").select($"companyId", $"fullAddress".alias("currentAddress"))

val dims = List("names", "objectives", "addresses", "positions", "reference_numbers", "capital")
var dfWide = dfCompanies.join(dfLatestName, "companyId", "outer").join(dfLatestAddress, "companyId", "outer").join(dfLatestObjective, "companyId", "outer")
for(i <- 0 until dims.length) {
  val tbl = dims(i)
  val dfTmp = aggColumnsToList(spark.table(s"hreg_star_dim_$tbl"))
  dfWide = dfWide.join(dfTmp, "companyId", "outer").withColumnRenamed("list", tbl)
}

val dfWideFinal = dfWide.withColumnRenamed("reference_numbers", "referenceNumbers")

//display(dfWideFinal)
dfWideFinal.coalesce(1).write.mode("overwrite")/*.option("compression", "bzip2")*/.json("/FileStore/hreg_export.json")
dfWideFinal.coalesce(1).write.mode("overwrite")/*.option("compression", "bzip2")*/.format("avro").save("/FileStore/hreg_export.avro")

// COMMAND ----------

display(
  dfWideFinal
)

// COMMAND ----------



// COMMAND ----------

val totalCount = dfWideFinal.count().toFloat
dfWideFinal.cache()
dfWideFinal.drop("companyId").columns.foreach(c => {
  val nShare = 1.0 - (dfWideFinal.where(s"$c IS NULL").count() / totalCount)
  println(s"$c: $nShare")
})

// COMMAND ----------

dfWideFinal.printSchema()