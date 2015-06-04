# Databricks notebook source exported at Thu, 4 Jun 2015 02:07:58 UTC
display(dbutils.fs.ls("/mnt/paco/events"))

# COMMAND ----------

evt = sc.textFile("/mnt/paco/events") \
 .map(lambda x: x.split("\t")) \
 .filter(lambda x: x[0] != "date")

# COMMAND ----------

evt.take(2)

# COMMAND ----------

e = evt.map(lambda p: (p[0], p[2], p[6], p[6].find("/spark-users/"),)).filter(lambda x: x[3] > -1)
e_df = e.toDF()
e_df.registerTempTable("e")
e_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM e

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import *
import re

def getBaseUrl (url):
  p = re.compile("^(http?\:\/\/.*\.meetup\.com\/[\w\-]+\/).*$")
  m = p.match(url)

  if m:
    return m.group(1)
  else:
    return ""

def convInt (reg):
  try:
  	return int(reg)
  except ValueError:
    return 0

evt_schema = StructType([
  StructField("date", StringType(), True),
  StructField("time", StringType(), True),
  StructField("title", StringType(), True),
  StructField("speakers", StringType(), True),
  StructField("affil", StringType(), True),
  StructField("addr", StringType(), True),
  StructField("url", StringType(), True),
  StructField("base_url", StringType(), True),
  StructField("reg", IntegerType(), True),
  StructField("city", StringType(), True)
])

evt_rdd = evt.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], getBaseUrl(p[6]), convInt(p[8]), p[9]))
evt_df = sqlContext.createDataFrame(evt_rdd, evt_schema)
evt_df.registerTempTable("events")

# COMMAND ----------

# MAGIC %sql SELECT date, city, title, speakers, affil FROM events LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC # Which are the most active meetups, by number of events?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  base_url, COUNT(*) AS num 
# MAGIC FROM events
# MAGIC WHERE base_url <> ""
# MAGIC GROUP BY base_url
# MAGIC ORDER BY num DESC

# COMMAND ----------

# MAGIC %md
# MAGIC # Which meetups have the highest attendance?

# COMMAND ----------

# ug! perhaps there's a bug in PySpark SQL that munges data in the form of 
# "Fubar Jones" <jones@fubar.org>
# because only the data within quotes shows in the SQL query results

def splitContact (contact):
  p = re.compile("^(.*)\s+\<(.*)\>.*$")
  m = p.match(contact)

  if m:
    return [m.group(1).replace('"', ''), m.group(2)]
  else:
    return ["", ""]

# COMMAND ----------

meta_rdd = sc.textFile("/mnt/paco/meetup_metadata.tsv") \
 .map(lambda x: x.split("\t")) \
 .filter(lambda x: x[0] != "region") \
 .map(lambda x: [x[5], x[0]] + splitContact(x[6]))
  
meta_df = sqlContext.createDataFrame(meta_rdd, ["base_url", "region", "contact", "email"])
meta_df.registerTempTable("meta")

meta_rdd.take(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM meta WHERE contact <> "" LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM events
# MAGIC WHERE events.base_url = "http://www.meetup.com/spark-users/"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  events.base_url, SUM(events.reg) AS attendance, COUNT(*) AS num, events.city, meta.region, meta.contact, meta.email
# MAGIC FROM events LEFT JOIN meta ON (events.base_url = meta.base_url)
# MAGIC WHERE events.base_url <> ""
# MAGIC GROUP BY events.base_url, meta.region, events.city, meta.contact, meta.email
# MAGIC ORDER BY attendance DESC

# COMMAND ----------

# MAGIC %md
# MAGIC # Which are the top cities?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  city, COUNT(*) AS num 
# MAGIC FROM events 
# MAGIC GROUP BY city
# MAGIC ORDER BY num DESC
# MAGIC LIMIT 30

# COMMAND ----------

# MAGIC %md
# MAGIC # Which are the top company affiliations?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  affil, COUNT(*) AS num 
# MAGIC FROM events 
# MAGIC GROUP BY affil
# MAGIC ORDER BY num DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC # Which companies are the most engaged with community events?

# COMMAND ----------

evt_df.printSchema()

# COMMAND ----------

sql = """
SELECT affil, COUNT(*) AS evt_count, MIN(date) AS earliest, MAX(date) AS latest
FROM events 
GROUP BY affil 
"""

from dateutil import parser

def days_hours_minutes (td):
    return float(td.days) + float(td.seconds) / 3600 + (float(td.seconds) / 60) % 60

lead_rdd = sqlContext.sql(sql) \
 .map(lambda x: (x[0], int(x[1]), days_hours_minutes(parser.parse(x[3]) - parser.parse(x[2]))))

lead_schema = StructType([
  StructField("affil", StringType(), True),
  StructField("count", IntegerType(), True),
  StructField("duration", FloatType(), True)
])

lead_df = sqlContext.createDataFrame(lead_rdd, lead_schema)
lead_df.registerTempTable("leaders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT affil, count, duration
# MAGIC FROM leaders
# MAGIC ORDER BY count DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %run /_SparkCamp/Exsto/pythonUtils

# COMMAND ----------

# MAGIC %md Importing usefull libraries

# COMMAND ----------

from ggplot import *
from pyspark.mllib.clustering import KMeans
from numpy import array
from math import log, exp
from pyspark.mllib.linalg import DenseVector, Vectors
from pyspark.sql import Row
import pandas

# COMMAND ----------

# MAGIC %md Functions to transform SchemaRDD to dictionary and dataframe

# COMMAND ----------

def toDict(rows):
  return [dict(zip(r.__FIELDS__, r)) for r in rows]

def toDataFrame(rows):
  return pandas.DataFrame(toDict(rows))

# COMMAND ----------

# MAGIC %md Functions for sampling

# COMMAND ----------

def getSampleRate(d, num):
  count = int(d['count'])
  year = d['year']
  ratio = 1.0 if count < num else 1.0 * num / count
  return (year, ratio)

def getFractions(c, num):
  return dict([getSampleRate(d, num) for d in toDict(c)])

# COMMAND ----------

sql = """
SELECT affil, count, duration
FROM leaders
ORDER BY count DESC
LIMIT 10
"""

df = sqlContext.sql(sql).toPandas()

# COMMAND ----------

df.head()

# COMMAND ----------

plot = ggplot(df, aes(x='duration', y='count', label='affil')) \
+ geom_text(hjust=0.5, vjust=0.5) \
+ geom_point()
display(plot)

# COMMAND ----------

# MAGIC %md
# MAGIC # Who are the most frequent speakers?

# COMMAND ----------

from operator import add

speak_rdd = evt.flatMap(lambda x: x[3].split(", ")) \
 .map(lambda x: (x, 1)).reduceByKey(add)

speak_schema = StructType([
  StructField("speaker", StringType(), True),
  StructField("count", IntegerType(), True)
])

speak_df = sqlContext.createDataFrame(speak_rdd, speak_schema)
speak_df.registerTempTable("speakers")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * 
# MAGIC FROM speakers
# MAGIC ORDER BY count DESC
# MAGIC LIMIT 23
