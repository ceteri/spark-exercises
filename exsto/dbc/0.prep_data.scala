// Databricks notebook source exported at Mon, 9 Feb 2015 04:37:26 UTC
// MAGIC %md ## Set up your S3 credentials
// MAGIC NB: URL encode since AWS SecretKey can contain "/" and other characters

// COMMAND ----------

dbutils.fs.unmount(s"/$MountName")

// COMMAND ----------

import java.net.URLEncoder
val AccessKey = "YOUR_ACCESS_KEY"
val SecretKey = URLEncoder.encode("YOUR_SECRET_KEY", "UTF-8")
val AwsBucketName = "paco.dbfs.public"
val MountName = "mnt/paco"

// COMMAND ----------

// MAGIC %md ## Mount the S3 bucket

// COMMAND ----------

dbutils.fs.mount(s"s3n://$AccessKey:$SecretKey@$AwsBucketName", s"/$MountName")

// COMMAND ----------

// MAGIC %md ## List the mounted contents

// COMMAND ----------

display(dbutils.fs.ls(s"/$MountName/exsto/parsed"))
