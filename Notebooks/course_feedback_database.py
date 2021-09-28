# Databricks notebook source
# MAGIC %md
# MAGIC ### Import libraries and create SparkSession

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession 

# COMMAND ----------

spark = SparkSession.builder.appName( "pandas to spark").getOrCreate() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract with PySpark

# COMMAND ----------

#jdbc properties 
jdbcHostname = "<server>.database.windows.net"
jdbcDatabase = "<database>" 
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : "<myusername>",
  "password" : "<mypassword>",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# COMMAND ----------

#Read in data with jdbc to Spark dataframe
Spdf = spark.read.jdbc(url=jdbcUrl, table="COURSE_FEEDBACK", properties=connectionProperties)

# COMMAND ----------

#Inspect
display(Spdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform with pandas

# COMMAND ----------

#Transform Spark dataframe to pandas dataframe
course_feedback = Spdf.toPandas()

# COMMAND ----------

#Inspect
course_feedback.info()

# COMMAND ----------

#Create Overall column
course_feedback['COURSE_EVALUATION'] = course_feedback.iloc[:, 4:7].mean(axis=1).round(2)
#Drop not needed columns
course_feedback.drop(['SETUP','MATERIAL','TEACHER'], inplace= True, axis=1)

# COMMAND ----------

#Inspect
course_feedback.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load with PySpark

# COMMAND ----------

#Transform pandas dataframe to Spark dataframe
Spdf_overall=spark.createDataFrame(course_feedback) 

# COMMAND ----------

#Write data with jdbc to Azure SQL
Spdf_overall.write.jdbc(url=jdbcUrl, table="COURSE_FEEDBACK_OVERALL", mode = "overwrite",properties=connectionProperties)
