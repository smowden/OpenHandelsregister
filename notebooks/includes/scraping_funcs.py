# Databricks notebook source
# MAGIC %pip install aiohttp aiofile tqdm nest-asyncio

# COMMAND ----------

import asyncio
import aiohttp
import json
from tqdm.asyncio import trange, tqdm
from typing import Dict
from datetime import datetime

from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import *

import nest_asyncio
nest_asyncio.apply()

ALL_STATES = [
    'be', 'bw', 'by', 'br', 'hb', 'hh', 'he', 'mv', 'ni', 'nw', 'rp', 'sl', 'sn', 'st', 'sh', 'th'
]
tmp_filename = f"/dbfs/hr_data/latest.{datetime.now().timestamp()}.jsonl"

def get_default_max_ids() -> Dict:
  ones = [1 for _ in range(len(ALL_STATES))]
  d = dict(zip(ALL_STATES, ones))
  d["sh"] = 7831 # custom start for
  
  return d

def init_dirs():
    pass

def get_max_ids_for_states(df_handelsregister_processed: DataFrame) -> Dict:
    max_ids = (
        df_handelsregister_processed
            .groupBy("landAbk")
            .max("id")
            .select(
                F.col("landAbk").alias("land_abk"),
                F.col("max(id)").alias("max_id")
            )
            .orderBy("max_id")
    )
    intermediate_dict = max_ids.toPandas().to_dict(orient="list")
    return dict(zip(intermediate_dict["land_abk"], intermediate_dict["max_id"]))




async def fetch_missing_publications(max_ids: Dict, max_records_per_state: int = 15000, only_states = None):
    max_ids = {**get_default_max_ids(), **max_ids}
    
    if only_states is None:
      states = max_ids.keys()
    else:
      states = only_states
    afp = open(tmp_filename, 'w+')
    async with aiohttp.ClientSession() as session:
        for state in states:
            failcount = 0
            for i in trange(max_ids[state], max_ids[state] + max_records_per_state, desc=state):
                async with session.get(
                        f'https://www.handelsregisterbekanntmachungen.de/skripte/hrb.php?rb_id={i}&land_abk={state}') as response:
                    html = await response.text()
                    if "Falsche Parameter" in html or "falscher Skriptaufruf!!!" in html:
                        failcount += 1
                        if failcount >= 200:
                            break
                        else:
                            continue
                    else:
                        failcount = 0
                    # print("Body:", html)
                    afp.write(json.dumps({
                        "landAbk": state,
                        "id": i,
                        "html": html
                    }))
                    afp.write("\n")
                    afp.flush()

def run_scraping_loop(df_publications_raw: DataFrame, max_records_per_state:int=15000, only_states=None):
    max_ids = get_max_ids_for_states(df_publications_raw)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(fetch_missing_publications(max_ids, max_records_per_state, only_states))


def write_new_publications_to_raw_table(inputDataframe: DataFrame, rawDeltaTable: DeltaTable):
    (
        rawDeltaTable
            .alias("hr_raw")
            .merge(
                inputDataframe.alias("hr_new"),
                "hr_raw.id = hr_new.id AND hr_raw.land_abk = hr_new.landAbk"
            )
            .whenNotMatchedInsert(
                values={
                    "ingest_date": F.current_date(),
                    "id": "hr_new.id",
                    "land_abk": "hr_new.landAbk",
                    "html": "hr_new.html",
                    "status": F.lit("unprocessed")
                }
            )
            .execute()
    )


# COMMAND ----------

# MAGIC  %scala
# MAGIC import net.ruippeixotog.scalascraper.browser.JsoupBrowser
# MAGIC import net.ruippeixotog.scalascraper.browser.JsoupBrowser._
# MAGIC import net.ruippeixotog.scalascraper.scraper.ContentExtractors.text
# MAGIC import net.ruippeixotog.scalascraper.dsl.DSL._
# MAGIC import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
# MAGIC import net.ruippeixotog.scalascraper.dsl.DSL.Parse._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import io.delta.tables._
# MAGIC  import org.apache.spark.sql.DataFrame
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC def getFeatures(id:Long, landAbk: String, htmlString: String): (Long, String, String, Option[String], Option[String], Option[String], String) = {
# MAGIC   //try {
# MAGIC     val browser = JsoupBrowser()
# MAGIC     val doc = browser.parseString(htmlString).asInstanceOf[JsoupDocument]
# MAGIC     
# MAGIC     val referenceNumber = doc >> text("table > tbody > tr:nth-child(1) > td:nth-child(1) > nobr > u")
# MAGIC     val typeOfPublication = doc >?> text("table > tbody > tr:nth-child(3) > td")
# MAGIC     val dateOfPublication = doc >?> text("table > tbody > tr:nth-child(4) > td")
# MAGIC     val publicationBody = doc >?> text("table > tbody > tr:nth-child(6) > td")
# MAGIC 
# MAGIC     val fullText = doc >> text("table")
# MAGIC     
# MAGIC     (id, landAbk, referenceNumber, typeOfPublication, dateOfPublication, publicationBody, fullText)
# MAGIC   //}catch {
# MAGIC   //      case e: Exception => (null, marketplaceId, null, null, null, null, null, null, null, null, null, null)
# MAGIC   //}
# MAGIC }
# MAGIC 
# MAGIC // move this somewhere else
# MAGIC spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
# MAGIC 
# MAGIC def extractHtmlFromRaw(dfRaw: DataFrame) = {
# MAGIC     val rddProcessed = dfRaw
# MAGIC     .select("id", "land_abk", "html")
# MAGIC     .rdd.map(row => getFeatures(row.getAs[Long]("id"), row.getAs[String]("land_abk"), row.getAs[String]("html")))
# MAGIC 
# MAGIC    rddProcessed
# MAGIC     .toDF("id", "landAbk", "referenceNumber", "typeOfPublication", "dateOfPublication", "publicationBody", "fullText")
# MAGIC     .withColumn("referenceNumber", regexp_replace(regexp_replace($"referenceNumber", " Aktenzeichen:", ""), "Amtsgericht ", ""))
# MAGIC     //.withColumn("lowerPublicationBody", lower($"publicationBody")) // TODO
# MAGIC }
# MAGIC 
# MAGIC def appendProcessedToDeltaTable(dfUpdates: DataFrame, deltaProcessedTable: DeltaTable) = {
# MAGIC   deltaProcessedTable
# MAGIC     .as("hr_processed")
# MAGIC     .merge(
# MAGIC       dfUpdates.as("hr_updates"),
# MAGIC       "hr_processed.id = hr_updates.id AND hr_processed.landAbk = hr_updates.landAbk")
# MAGIC     .whenNotMatched
# MAGIC     .insertExpr(
# MAGIC       Map(
# MAGIC         "id" -> "hr_updates.id",
# MAGIC         "landAbk" -> "hr_updates.landAbk",
# MAGIC         "referenceNumber" -> "hr_updates.referenceNumber",
# MAGIC         "typeOfPublication" -> "hr_updates.typeOfPublication",
# MAGIC         "dateOfPublication" -> "hr_updates.dateOfPublication",
# MAGIC         "publicationBody" -> "hr_updates.publicationBody",
# MAGIC         //"lowerPublicationBody" -> "hr_updates.lowerPublicationBody",
# MAGIC         "fullText" -> "hr_updates.fullText"
# MAGIC       )
# MAGIC     )
# MAGIC     .execute()
# MAGIC   
# MAGIC   DeltaTable.forName(spark, "hreg_bronze_raw_html_w_metadata").updateExpr("status = 'unprocessed'", Map("status" -> "'processed'"))
# MAGIC }
# MAGIC val createProcessedTableIfNotExists = (sourceTableName: String) => spark.sql(s"CREATE TABLE IF NOT EXISTS hreg_bronze_publications AS (SELECT * FROM $sourceTableName LIMIT 0);")