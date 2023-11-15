# UDF with string manipulation
import findspark

findspark.init()

from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PySparkUDFExample").getOrCreate()

# Sample data
data02 = [("alice",), ("bob",), ("carol",)]
schema02 = ["name"]

# Create a DataFrame
df2 = spark.createDataFrame(data02, schema=schema02)


# Define a UDF to capitalize the first letter of a string
@udf(StringType())
def capitalize_udf(name):
    return name.capitalize()


# Apply the UDF to a column
df_with_capitalized = df2.withColumn("capitalized_name", capitalize_udf(df2["name"]))
df_with_capitalized.show()
