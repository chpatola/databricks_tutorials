# Databricks notebook source
import pyspark.pandas as ps

# COMMAND ----------

# MAGIC %run ./Functions/connect_azure_blob

# COMMAND ----------

#Mount rawdata-courses blob
mount_blob(storage_account="chpatoladatabricks",
           kv_scope="chpatoladatabricks",
           kv_secret="chpatoladatabricks-storage-account-key",
           blob="rawdata-courses")

# COMMAND ----------

#Mount transformeddata-courses blob
mount_blob(storage_account="chpatoladatabricks",
           kv_scope="chpatoladatabricks",
           kv_secret="chpatoladatabricks-storage-account-key",
           blob="transformeddata-courses")

# COMMAND ----------

#Read in and inspect raw data from blob
course_feedback = ps.read_csv(
    '/mnt/rawdata-courses/course_feedback.csv')
course_feedback.info()

# COMMAND ----------

# Transform and inspect data
ps.set_option('compute.ops_on_diff_frames', True)
course_feedback['Overall'] = course_feedback.iloc[:, 4:7].mean(axis=1).round(2)
course_feedback.head(5)

# COMMAND ----------

#Save transformed data to blob
course_feedback.to_csv('/mnt/transformeddata-courses/course_feedback_overall.csv',index=False)
course_feedback_overall = ps.read_csv('/mnt/transformeddata-courses/course_feedback_overall.csv')
course_feedback_overall.head(5)

# COMMAND ----------

#Unmount used blobs
unmount_blob("rawdata-courses")
unmount_blob("transformeddata-courses")
