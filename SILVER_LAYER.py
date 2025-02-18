# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Script

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Access using app

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.awstoragedatalake7.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awstoragedatalake7.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.awstoragedatalake7.dfs.core.windows.net", "ad45c71e-8230-4906-bf74-58768cfaac9a")
spark.conf.set("fs.azure.account.oauth2.client.secret.awstoragedatalake7.dfs.core.windows.net", ".sg8Q~89LNgLEsZfNSmF3oP~ak-cj3Esk2jsWb-Z")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awstoragedatalake7.dfs.core.windows.net", "https://login.microsoftonline.com/087ae903-1732-4222-8ea2-109d7b1d1102/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Data

# COMMAND ----------

df_cal = spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@awstoragedatalake7.dfs.core.windows.net/AdventureWorks_Calendar')


# COMMAND ----------

df_customer = spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@awstoragedatalake7.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

df_procat = spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@awstoragedatalake7.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

df_pro = spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@awstoragedatalake7.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

df_ret = spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@awstoragedatalake7.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

df_sale = spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@awstoragedatalake7.dfs.core.windows.net/AdventureWorks_Sales*')

# COMMAND ----------

 df_ter = spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@awstoragedatalake7.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

 df_sub = spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@awstoragedatalake7.dfs.core.windows.net/Product_Subcategories')

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORMATIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calender

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal = df.withColumn('Month',month(col('Date')))\
      .withColumn('Year',year(col('Date')))
df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet').mode('append').option("path","abfss://silver@awstoragedatalake7.dfs.core.windows.net/AdventureWorks_Calendar").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer

# COMMAND ----------

df_customer.display()

# COMMAND ----------

df_customer =df_customer.withColumn("fullName",concat(col('Prefix'),lit(''),col('FirstName'),lit(''),col('LastName'))).display()

# COMMAND ----------

df_customer.write.format('parquet').mode('append').option("path","abfss://silver@awstoragedatalake7.dfs.core.windows.net/AdventureWorks_Customers").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sub Categories

# COMMAND ----------

df_sub.display()

# COMMAND ----------

df_sub.write.format('parquet').mode('append').option("path","abfss://silver@awstoragedatalake7.dfs.core.windows.net/Product_Subcategories").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
.withColumn('ProductName',split(col('ProductName'),' ')[0])

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet').mode('append').option("path","abfss://silver@awstoragedatalake7.dfs.core.windows.net/AdventureWorks_Products").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Returns

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet').mode('append').option("path","abfss://silver@awstoragedatalake7.dfs.core.windows.net/AdventureWorks_Returns").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Territories

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format('parquet').mode('append').option("path","abfss://silver@awstoragedatalake7.dfs.core.windows.net/AdventureWorks_Territories").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SALES

# COMMAND ----------

df_sale.display()  

# COMMAND ----------

# MAGIC %md
# MAGIC ### sales using Aggregate function
# MAGIC

# COMMAND ----------

df_sale = df_sale.withColumn('StockDate', to_timestamp('StockDate'))

# COMMAND ----------

df_sale = df_sale.withColumn('OrderNumber', regexp_replace(col('OrderNumber'),'S', 'T'))

# COMMAND ----------

df_sale = df_sale.withColumn('multiply', col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sale.display()

# COMMAND ----------

df_sale.write.format('parquet').mode('append').option("path","abfss://silver@awstoragedatalake7.dfs.core.windows.net/AdventureWorks_Sales").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Analysis
# MAGIC

# COMMAND ----------

df_sale.groupBy('OrderDate').agg(count('OrderNumber').alias('total order')).display() 

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_ter.display()  

# COMMAND ----------

