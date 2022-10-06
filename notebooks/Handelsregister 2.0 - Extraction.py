# Databricks notebook source
# MAGIC %sh
# MAGIC rm /dbfs/hr_data/*

# COMMAND ----------

# MAGIC %run ./includes/scraping_funcs

# COMMAND ----------

from pyspark.sql.window import Window

sc.setJobDescription("Get current scraping progress")
windowSpec = Window.partitionBy("landAbk").orderBy(F.desc("id"))
seq = F.row_number().over(windowSpec)

dfProgress = spark.table("hreg_bronze_publications")

dfLatest = dfProgress.withColumn("seq", seq).where(F.col("seq") == 1).select("landAbk", "dateOfPublication", "id")
display(dfLatest)

# todo: ensure distinct

# COMMAND ----------

run_scraping_loop(spark.table("hreg_bronze_publications"), 5000)  #only_states=['nw']

delta_publications_raw = DeltaTable.forName(spark, "hreg_bronze_raw_html_w_metadata")
df_new_publications = spark.read.format("json").load("/hr_data/")
write_new_publications_to_raw_table(df_new_publications, delta_publications_raw)

# COMMAND ----------

df_new_publications = spark.read.format("json").load("/hr_data/")
display(df_new_publications.groupBy("landAbk").count())

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lh /dbfs/hr_data/

# COMMAND ----------

# MAGIC %scala
# MAGIC val dfRaw = spark.table("hreg_bronze_raw_html_w_metadata").filter("status = 'unprocessed'")
# MAGIC val dfProcessed = extractHtmlFromRaw(dfRaw)
# MAGIC dfProcessed.createOrReplaceTempView("tmp_hr_processed")
# MAGIC createProcessedTableIfNotExists("tmp_hr_processed")
# MAGIC 
# MAGIC appendProcessedToDeltaTable(dfProcessed, DeltaTable.forName(spark, "hreg_bronze_publications"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM handelsregister_processed WHERE landAbk = 'sh'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS neueintragungen AS (
# MAGIC   SELECT * 
# MAGIC   FROM hr_p
# MAGIC   WHERE typeOfPublication = "Neueintragungen" AND referenceNumber NOT RLIKE "VR \\d+(.*)"
# MAGIC   DISTRIBUTE BY id, landAbk
# MAGIC )

# COMMAND ----------

# MAGIC %run ./includes/util