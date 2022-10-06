// Databricks notebook source
https://docs.databricks.com/spark/latest/graph-analysis/graphframes/graph-analysis-tutorial.html

https://graphframes.github.io/graphframes/docs/_site/user-guide.html#connected-components

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(DISTINCT concat(firstName, ' ', lastName)) FROM persons

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TABLE persons AS (
// MAGIC   WITH semi_clean_persons AS (
// MAGIC     SELECT 
// MAGIC       trim(firstName) AS firstName,
// MAGIC       trim(lastName) AS lastName,
// MAGIC       trim(birthDate) AS birthDate
// MAGIC     FROM persons 
// MAGIC       WHERE 
// MAGIC       firstName NOT LIKE '%*%' 
// MAGIC       AND firstName NOT LIKE '%:%'
// MAGIC       AND firstName != ''
// MAGIC       AND lastName != '' AND lastName NOT LIKE '%*%' AND birthDate IS NOT NULL AND length(firstName) < 70 AND length(lastName) < 60 AND length(birthDate) = 10 AND lastName NOT LIKE '%Änderung%' AND lastName NOT LIKE '%:%'
// MAGIC       ORDER BY length(lastName) ASC
// MAGIC   )
// MAGIC   SELECT 
// MAGIC     concat(firstName, '-', lastName, '-', hex(hash(firstName, lastName, birthDate))) AS slug,
// MAGIC     firstName,
// MAGIC     lastName,
// MAGIC     birthDate
// MAGIC   FROM semi_clean_persons
// MAGIC   GROUP BY firstName, lastName, birthDate
// MAGIC   
// MAGIC );
// MAGIC 
// MAGIC CREATE OR REPLACE TABLE person_company_rels AS (
// MAGIC   SELECT 
// MAGIC     slug AS personSlug,
// MAGIC     stdRefNoUni AS entityId,
// MAGIC     startDate,
// MAGIC     endDate,
// MAGIC     city,
// MAGIC     roleFilledClean
// MAGIC   FROM roles_people
// MAGIC   INNER JOIN persons USING (firstName, lastName, birthDate)
// MAGIC );

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM persons WHERE firstName = 'Mohammad Reza' AND lastName = 'Abdolmohammadi' ORDER BY lastName, firstName 

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM persons ORDER BY lastName, firstName

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM persons 
// MAGIC WHERE 
// MAGIC firstName NOT LIKE '%*%' 
// MAGIC AND firstName NOT LIKE '%:%'
// MAGIC AND firstName != ''
// MAGIC AND lastName != '' AND lastName NOT LIKE '%*%' AND birthDate IS NOT NULL AND length(firstName) < 70 AND length(lastName) < 60 AND length(birthDate) = 10 AND lastName NOT LIKE '%Änderung%' AND lastName NOT LIKE '%:%'
// MAGIC ORDER BY length(lastName) ASC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC   component,
// MAGIC   count(*)
// MAGIC FROM component_ids
// MAGIC GROUP BY 1
// MAGIC ORDER BY 2 DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM component_ids WHERE component = 32

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC   firstName,
// MAGIC   lastName,
// MAGIC   birthDate,
// MAGIC   count(*)
// MAGIC FROM persons
// MAGIC LEFT JOIN person_company_rels ON slug = personSlug
// MAGIC LEFT JOIN component_ids ON 
// MAGIC GROUP BY 1, 2, 3
// MAGIC ORDER BY 4 DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC   *
// MAGIC FROM persons
// MAGIC LEFT JOIN person_company_rels ON slug = personSlug
// MAGIC WHERE firstName = 'Katja' AND lastName = 'Gogalla' AND birthDate = '11.04.1968'

// COMMAND ----------

import org.graphframes.GraphFrame

val vertexDf = spark.sql("""
SELECT
  slug as id,
  'person' AS vType,
  concat(firstName, ' ', lastName) as name
FROM persons
--WHERE birthDate IS NOT NULL AND length(lastName) > 0 AND length(firstName) > 0 AND slug NOT IN (SELECT id FROM out_degrees WHERE outDegree > 1000)
UNION
SELECT
  stdRefNoUni as slug,
  'company' AS vType,
  companyName AS name
FROM companies
""")

vertexDf.createOrReplaceTempView("vertex_df")

val edgeDf = spark.sql("""
SELECT
  personSlug as src,
  entityId as dst,
  roleFilledClean as relationship
  --endDate IS NULL as former
FROM person_company_rels WHERE personSlug IN (SELECT id FROM vertex_df)
""")

val g = GraphFrame(vertexDf, edgeDf)

spark.sparkContext.setCheckpointDir("dbfs:/graphframe/")
val result = g.connectedComponents.run()
result.write.mode("overwrite").saveAsTable("component_ids") // .select("id", "component").orderBy("component")

// COMMAND ----------

g.outDegrees.cache().write.mode("overwrite").saveAsTable("out_degrees")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT component, count(*) FROM component_ids GROUP BY 1 ORDER BY 2 DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM component_ids WHERE component = 32

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM out_degrees ORDER BY outDegree DESC

// COMMAND ----------

g.inDegrees.printSchema()

// COMMAND ----------

import org.apache.spark.sql.functions._

spark.table("companies")
.repartition(1)
.write
.mode("overwrite").option("quoteAll", true).option("delimiter", "#").option("treatEmptyValuesAsNulls", "true").csv("dbfs:/compyre/companies/")


spark.table("publications_classified")
.withColumn("publicationBody", lit(""))
.withColumn("fullText", substring($"fullText", 1, 50))
.repartition(1)
.write
.mode("overwrite").option("quoteAll", true).option("delimiter", "#").option("treatEmptyValuesAsNulls", "true").csv("dbfs:/compyre/publications_classified/")

spark.table("persons")
.repartition(1)
.write
.mode("overwrite").option("quoteAll", true).option("delimiter", "#").option("treatEmptyValuesAsNulls", "true").csv("dbfs:/compyre/persons/")


spark.table("person_company_rels")
.repartition(1)
.write
.mode("overwrite").option("quoteAll", true).option("delimiter", "#").option("treatEmptyValuesAsNulls", "true").csv("dbfs:/compyre/person_company_rels/")



spark.table("component_ids")
.repartition(1)
.write
.mode("overwrite").option("quoteAll", true).option("delimiter", "#").option("treatEmptyValuesAsNulls", "true").csv("dbfs:/compyre/component_ids/")

//spark.table("persons").write.mode("overwrite").csv("dbfs:/compyre/persons/")
//spark.table("person_company_rels").write.mode("overwrite").csv("dbfs:/compyre/person_company_rels/")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM publications_classified;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM publications_classified WHERE referenceNumber IN (
// MAGIC 'Frankfurt am Main HRB 103206',
// MAGIC 'Charlottenburg (Berlin) HRB 194680 B',
// MAGIC 'Traunstein HRB 24070'
// MAGIC ) ORDER BY dateOfPublication

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM companies WHERE referenceNumber = 'Charlottenburg (Berlin) HRB 134456 B'

// COMMAND ----------

spark.table("events_processed").printSchema()

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM companies WHERE referenceNumber = 'Amberg HRA 3271'

// COMMAND ----------

// MAGIC %sql
// MAGIC show create table component_ids;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM persons WHERE firstName LIKE '*%'

// COMMAND ----------

// MAGIC %sh
// MAGIC #cat /dbfs/compyre/companies/*.csv |grep 'Amberg HRA 3271'
// MAGIC 
// MAGIC ls -lh /dbfs/compyre/publications_classified

// COMMAND ----------

// MAGIC %python
// MAGIC import psycopg2
// MAGIC import os
// MAGIC 
// MAGIC host = "db.vdkzdgizpfdbyrlmuswe.supabase.co"
// MAGIC password = "f8wp59kUeuXEMJqajC"
// MAGIC port = 5432
// MAGIC #postgres://postgres:[YOUR-PASSWORD]@db.vdkzdgizpfdbyrlmuswe.supabase.co:6543/postgres
// MAGIC 
// MAGIC conn = psycopg2.connect(f"host={host} dbname=postgres user=postgres password={password} port={port}")
// MAGIC cur = conn.cursor()
// MAGIC 
// MAGIC 
// MAGIC cur.execute("""
// MAGIC DROP TABLE IF EXISTS companies;
// MAGIC """)
// MAGIC 
// MAGIC cur.execute("""
// MAGIC CREATE TABLE companies (
// MAGIC     referenceNumber varchar PRIMARY KEY,
// MAGIC     companyName text,
// MAGIC     address text,
// MAGIC     objective text,
// MAGIC     place text,
// MAGIC     stammkapital text,
// MAGIC     foundingDate varchar,
// MAGIC     dissolutionDate varchar
// MAGIC )
// MAGIC """)
// MAGIC conn.commit()
// MAGIC 
// MAGIC for file in os.listdir("/dbfs/compyre/companies/"):
// MAGIC   if file.endswith("csv") and not "*" in file:
// MAGIC     with open("/dbfs/compyre/companies/"+file, 'r') as f:
// MAGIC       cur.copy_expert("COPY companies(referenceNumber, companyName, address, objective, place, stammkapital, foundingDate, dissolutionDate) FROM STDIN WITH DELIMITER '#' NULL AS '' CSV QUOTE AS '\"' ESCAPE AS '\\'", f)
// MAGIC       
// MAGIC conn.commit()

// COMMAND ----------

// MAGIC %python
// MAGIC import psycopg2
// MAGIC import os
// MAGIC 
// MAGIC host = "db.vdkzdgizpfdbyrlmuswe.supabase.co"
// MAGIC password = "f8wp59kUeuXEMJqajC"
// MAGIC port = 5432
// MAGIC #postgres://postgres:[YOUR-PASSWORD]@db.vdkzdgizpfdbyrlmuswe.supabase.co:6543/postgres
// MAGIC 
// MAGIC conn = psycopg2.connect(f"host={host} dbname=postgres user=postgres password={password} port={port}")
// MAGIC cur = conn.cursor()
// MAGIC 
// MAGIC #CREATE TABLE `default`.`person_company_rels` ( `personSlug` STRING, `entityId` STRING, `startDate` DATE, `endDate` DATE, `city` STRING, `roleFilledClean` STRING) USING delta
// MAGIC 
// MAGIC 
// MAGIC cur.execute("""
// MAGIC DROP TABLE IF EXISTS persons;
// MAGIC """)
// MAGIC 
// MAGIC cur.execute("""
// MAGIC CREATE TABLE `persons` ( `slug` STRING PRIMARY KEY, `firstName` STRING, `lastName` STRING, `birthDate` STRING)
// MAGIC """.replace("STRING", "TEXT").replace("DATE", "VARCHAR").replace("`", ""))
// MAGIC conn.commit()
// MAGIC 
// MAGIC cString = ",".join(spark.table("persons").columns)
// MAGIC 
// MAGIC 
// MAGIC for file in os.listdir("/dbfs/compyre/persons/"):
// MAGIC   if file.endswith("csv") and not "*" in file:
// MAGIC     with open("/dbfs/compyre/persons/"+file, 'r') as f:
// MAGIC       cur.copy_expert(f"COPY persons({cString}) FROM STDIN WITH DELIMITER '#' NULL AS '' CSV QUOTE AS '\"' ESCAPE AS '\\'", f)
// MAGIC       
// MAGIC conn.commit()

// COMMAND ----------

// MAGIC %python
// MAGIC import psycopg2
// MAGIC import os
// MAGIC 
// MAGIC host = "db.vdkzdgizpfdbyrlmuswe.supabase.co"
// MAGIC password = "f8wp59kUeuXEMJqajC"
// MAGIC port = 5432
// MAGIC #postgres://postgres:[YOUR-PASSWORD]@db.vdkzdgizpfdbyrlmuswe.supabase.co:6543/postgres
// MAGIC 
// MAGIC conn = psycopg2.connect(f"host={host} dbname=postgres user=postgres password={password} port={port}")
// MAGIC cur = conn.cursor()
// MAGIC 
// MAGIC 
// MAGIC cur.execute("""
// MAGIC DROP TABLE IF EXISTS person_company_rels;
// MAGIC """)
// MAGIC 
// MAGIC cur.execute("""
// MAGIC CREATE TABLE `default`.`person_company_rels` ( `personSlug` STRING , `entityId` STRING, `startDate` DATE, `endDate` DATE, `city` STRING, `roleFilledClean` STRING) USING delta
// MAGIC """.replace("STRING", "VARCHAR").replace("DATE", "VARCHAR").replace("`", "").replace("USING delta", "").replace("default.", ""))
// MAGIC conn.commit()
// MAGIC 
// MAGIC cString = ",".join(spark.table("person_company_rels").columns)
// MAGIC 
// MAGIC 
// MAGIC for file in os.listdir("/dbfs/compyre/person_company_rels/"):
// MAGIC   if file.endswith("csv") and not "*" in file:
// MAGIC     with open("/dbfs/compyre/person_company_rels/"+file, 'r') as f:
// MAGIC       cur.copy_expert(f"COPY person_company_rels({cString}) FROM STDIN WITH DELIMITER '#' NULL AS '' CSV QUOTE AS '\"' ESCAPE AS '\\'", f)
// MAGIC       
// MAGIC conn.commit()

// COMMAND ----------

// MAGIC %python
// MAGIC import psycopg2
// MAGIC import os
// MAGIC 
// MAGIC host = "db.vdkzdgizpfdbyrlmuswe.supabase.co"
// MAGIC password = "f8wp59kUeuXEMJqajC"
// MAGIC port = 5432
// MAGIC #postgres://postgres:[YOUR-PASSWORD]@db.vdkzdgizpfdbyrlmuswe.supabase.co:6543/postgres
// MAGIC 
// MAGIC conn = psycopg2.connect(f"host={host} dbname=postgres user=postgres password={password} port={port}")
// MAGIC cur = conn.cursor()
// MAGIC 
// MAGIC 
// MAGIC cur.execute("""
// MAGIC DROP TABLE IF EXISTS component_ids;
// MAGIC """)
// MAGIC 
// MAGIC cur.execute("""
// MAGIC CREATE TABLE `default`.`component_ids` ( `id` STRING, `vType` STRING, `name` STRING, `component` BIGINT) USING delta
// MAGIC """.replace("STRING", "VARCHAR").replace("DATE", "VARCHAR").replace("`", "").replace("USING delta", "").replace("default.", ""))
// MAGIC conn.commit()
// MAGIC 
// MAGIC cString = ",".join(spark.table("component_ids").columns)
// MAGIC 
// MAGIC 
// MAGIC for file in os.listdir("/dbfs/compyre/component_ids/"):
// MAGIC   if file.endswith("csv") and not "*" in file:
// MAGIC     with open("/dbfs/compyre/component_ids/"+file, 'r') as f:
// MAGIC       cur.copy_expert(f"COPY component_ids({cString}) FROM STDIN WITH DELIMITER '#' NULL AS '' CSV QUOTE AS '\"' ESCAPE AS '\\'", f)
// MAGIC       
// MAGIC conn.commit()

// COMMAND ----------

// MAGIC %python
// MAGIC import psycopg2
// MAGIC import os
// MAGIC 
// MAGIC host = "db.vdkzdgizpfdbyrlmuswe.supabase.co"
// MAGIC password = "f8wp59kUeuXEMJqajC"
// MAGIC port = 5432
// MAGIC #postgres://postgres:[YOUR-PASSWORD]@db.vdkzdgizpfdbyrlmuswe.supabase.co:6543/postgres
// MAGIC 
// MAGIC conn = psycopg2.connect(f"host={host} dbname=postgres user=postgres password={password} port={port}")
// MAGIC cur = conn.cursor()
// MAGIC 
// MAGIC 
// MAGIC cur.execute("""
// MAGIC DROP TABLE IF EXISTS publications_classified;
// MAGIC """)
// MAGIC 
// MAGIC cur.execute("""
// MAGIC CREATE TABLE `publications_classified` (
// MAGIC   `id` BIGINT,
// MAGIC   `landAbk` STRING,
// MAGIC   `court` STRING,
// MAGIC   `typeOfPublication` STRING,
// MAGIC   `dateOfPublication` DATE,
// MAGIC   `publicationBody` STRING,
// MAGIC   `fullText` STRING,
// MAGIC   `referenceNumber` STRING,
// MAGIC   `company_founded` BOOLEAN,
// MAGIC   `company_to_be_deleted` BOOLEAN,
// MAGIC   `company_deleted` BOOLEAN,
// MAGIC   `officers_changed` BOOLEAN,
// MAGIC   `officers_changed_alt` BOOLEAN,
// MAGIC   `prokura_ended` BOOLEAN,
// MAGIC   `officer_correction` BOOLEAN,
// MAGIC   `officers_removed` BOOLEAN,
// MAGIC   `officers_removed_alternative` BOOLEAN,
// MAGIC   `owner` BOOLEAN,
// MAGIC   `address` BOOLEAN,
// MAGIC   `in_liquidation` BOOLEAN,
// MAGIC   `liquidated` BOOLEAN,
// MAGIC   `insolvency` BOOLEAN,
// MAGIC   `branch_created` BOOLEAN,
// MAGIC   `branch_ended` BOOLEAN,
// MAGIC   `branch_change` BOOLEAN,
// MAGIC   `name_change` BOOLEAN,
// MAGIC   `company_change` BOOLEAN,
// MAGIC   `capital_increase` BOOLEAN,
// MAGIC   `capital_corrected` BOOLEAN,
// MAGIC   `court_moved` BOOLEAN,
// MAGIC   `objective_changed` BOOLEAN,
// MAGIC   `board_list_submitted` BOOLEAN,
// MAGIC   `capital_change` BOOLEAN,
// MAGIC   `legal_form_change` BOOLEAN,
// MAGIC   `company_agreement_changed` BOOLEAN,
// MAGIC   `gewinnabführungsvertrag` BOOLEAN,
// MAGIC   `ergebnisabführungsvertrag` BOOLEAN,
// MAGIC   `beherrschungsvertrag` BOOLEAN,
// MAGIC   `verschmelzungsvertrag` BOOLEAN,
// MAGIC   `judicial_deletion_announcement` BOOLEAN,
// MAGIC   `judicial_deletion` BOOLEAN,
// MAGIC   `judicial_deletion_cancelled` BOOLEAN,
// MAGIC   `company_defunct` BOOLEAN,
// MAGIC   `bylaws_changed` BOOLEAN,
// MAGIC   `split` BOOLEAN,
// MAGIC   `founding_date` DATE,
// MAGIC   `dissolution_date` DATE)
// MAGIC """.replace("STRING", "TEXT").replace("DATE", "VARCHAR").replace("`", ""))
// MAGIC 
// MAGIC cur.execute("SET statement_timeout = 10000000;")
// MAGIC conn.commit()
// MAGIC 
// MAGIC cString = ",".join(spark.table("publications_classified").columns)
// MAGIC for file in os.listdir("/dbfs/compyre/publications_classified/"):
// MAGIC   if file.endswith("csv") and not "*" in file:
// MAGIC     with open("/dbfs/compyre/publications_classified/"+file, 'r') as f:
// MAGIC       
// MAGIC       cur.copy_expert(f"COPY publications_classified({cString}) FROM STDIN WITH DELIMITER '#' NULL AS '' CSV QUOTE AS '\"' ESCAPE AS '\\'", f)
// MAGIC       
// MAGIC conn.commit()