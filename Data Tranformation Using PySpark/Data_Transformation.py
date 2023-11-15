import findspark

findspark.init()

import pyspark

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, min, max, mean, stddev

spark = SparkSession.builder.appName('Data_Transformation').getOrCreate()

# Normalized Value = (Value - Min)/(Max - Min)
# Standardized Value = (Value - Mean) / Standard Deviation

data = [
    ("Alice", 34),
    ("Bob", 45),
    ("Catherine", 29)
]

columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# df.show()

# Calculate Min and Max for Age
age_min = df.select(min(col("Age"))).collect()[0][0]
age_max = df.select((max(col("Age")))).collect()[0][0]

# print(age_min)
# print(age_max)

# Perform Normalization
df_normalized = df.withColumn("Age_Normalized", (col("Age") - age_min) / (age_max - age_min))

# df_normalized.show()

# Calculate Mean and Standard Deviation for Age
age_mean = df.select(mean(col("Age"))).collect()[0][0]
age_stddev = df.select(stddev(col("Age"))).collect()[0][0]

# Perform Standardization
df_standardized = df.withColumn("Age_Standardized", (col("Age") - age_mean) / age_stddev)

df_standardized.show()


