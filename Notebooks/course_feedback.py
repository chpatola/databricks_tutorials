# Databricks notebook source
# MAGIC %md 
# MAGIC ### Import libraries

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract and Inspect

# COMMAND ----------

course_feedback = pd.read_csv('/dbfs/FileStore/tables/course_feedback.csv')

# COMMAND ----------

course_feedback.info()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform and Inspect

# COMMAND ----------

course_feedback['Overall'] = course_feedback.iloc[:, 4:7].mean(axis=1).round(2)

# COMMAND ----------

course_feedback.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load 

# COMMAND ----------

course_feedback.to_csv('/dbfs/FileStore/tables/course_feedback_overall.csv',index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Control

# COMMAND ----------

course_feedback_overall = pd.read_csv('/dbfs/FileStore/tables/course_feedback_overall.csv')
course_feedback_overall.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What if your data is too large for pandas to handle?
# MAGIC If we would be dealing with Big Data, it would not be possible to let pandas take care of all the data wrangling. If we would still like to use pandas (or another library that cannot handle big data, for example matplotlib or seaborn) we can do part of the transformations in PySpark and part in pandas. This is possible if we decrease the dataframe size before using pandas, for example by grouping it or taking random samples from it. In our current case, it could be done like this with PySpark SQL:

# COMMAND ----------

#Read in csv as Spark dataframe.
SpDf = spark.read.csv('/FileStore/tables/course_feedback.csv', header=True, sep=',',inferSchema=True)

# COMMAND ----------

#Transform the Spark Dataframe to a Temporary View that can be handled with SQL
SpDf.createOrReplaceTempView('SpSql')

# COMMAND ----------

#Use SQL to transform the dataframe - here we group the course feedback per year and count averages
SpSqlGrouped = spark.sql('''
  SELECT
    CONCAT(YEAR, '-',MONTH) AS YEAR_MONTH,
    ROUND(MEAN(SETUP),2) AS SETUP_MEAN,
    ROUND(MEAN(MATERIAL),2) AS MATERIAL_MEAN,
    ROUND(MEAN(TEACHER),2) AS TEACHER_MEAN
  FROM SpSql
  GROUP BY YEAR, MONTH
  ORDER BY YEAR, MONTH
''')

# COMMAND ----------

# MAGIC %md
# MAGIC Now the PySpark SQL dataframe can be used as base table for further transformations or visualizations. 

# COMMAND ----------

# Transform the PySpark SQL dataframe to pandas
course_feedback_from_sql = SpSqlGrouped.select("*").toPandas()

#Visualize pandas dataframe in seaborn
import seaborn as sns
ax = sns.barplot(x="YEAR_MONTH", y="TEACHER_MEAN", data=course_feedback_from_sql)
