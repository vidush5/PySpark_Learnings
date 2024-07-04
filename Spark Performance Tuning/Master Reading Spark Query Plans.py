#!/usr/bin/env python
# coding: utf-8

# ## Master Reading Spark Query Plans
# 
# New notebook

# ## Imports & Configuration

# In[2]:


import warnings
warnings.filterwarnings("ignore")


# In[3]:


from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


# In[4]:


spark = SparkSession.builder.appName("QueryPlan").getOrCreate()


# ## Reading File

# In[5]:


transaction_file = "Files/data/data_skew/transactions.parquet"
df_transactions = spark.read.parquet(transaction_file)


# In[6]:


df_transactions.show(5, False)


# In[7]:


customers_file = "Files/data/data_skew/customers.parquet"
df_customers = spark.read.parquet(customers_file)


# In[8]:


df_customers.show(5, False)


# ### Narrow Transformation

# In[9]:


df_narrow_transformation = (
    df_customers
    .filter(F.col("city") == "boston")
    .withColumn("first_name", F.split("name", " ").getItem(0))
    .withColumn("last_name",F.split("name", " ").getItem(1))
    .withColumn("age", F.col("age") + F.lit(5))
    .select("cust_id", "first_name", "last_name", "age", "gender", "birthday")
)


df_narrow_transformation.show(5, False)


# In[10]:


df_narrow_transformation.explain(True)


# ### Wide Transformations

# ##### 1. Repartition

# In[11]:


df_transactions.rdd.getNumPartitions()


# In[12]:


df_transactions.repartition(24).explain(True)


# #### 2. Coalesce

# In[13]:


df_transactions.rdd.getNumPartitions()


# In[14]:


df_transactions.coalesce(1).explain(True)


# #### 3. Joins

# In[15]:


spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


# In[16]:


df_joined = (
    df_transactions.join(
        df_customers,
        how="inner",
        on="cust_id"
    )
)


# In[17]:


df_joined.explain(True)


# #### 04. GroupBy

# In[18]:


df_transactions.printSchema()


# In[19]:


df_city_counts = (
    df_transactions
    .groupBy("city")
    .count()
)


# In[20]:


df_city_counts.explain(True)


# In[21]:


df_txn_amt_city = (
    df_transactions
    .groupBy("city")
    .agg(F.sum("amt").alias("txn_amt"))
)


# In[22]:


df_txn_amt_city.explain(True)


# #### 05. GroupBy Count Distinct

# In[23]:


df_txn_per_city = (
    df_transactions
    .groupBy("cust_id")
    .agg(F.countDistinct("city").alias("city_count"))
)


# In[24]:


df_txn_per_city.show(5, False)
df_txn_per_city.explain(True)


# In[ ]:




