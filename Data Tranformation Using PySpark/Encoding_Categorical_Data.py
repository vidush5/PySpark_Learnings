import findspark

findspark.init()

import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Encoding').getOrCreate()

data = [
    ("Apple",),
    ("Banana",),
    ("Cherry",),
    ("Apple",),
    ("Banana",),
    ("Apple",)
]

columns = ["Fruit"]

df = spark.createDataFrame(data, columns)

#df.show()

# Label Encoding assigns a unique integer to each category in the categorical column

from pyspark.ml.feature import StringIndexer

# Initialize StringIndexer
indexer = StringIndexer(inputCol="Fruit", outputCol="Fruit_Index")

df_indexed = indexer.fit(df).transform(df)

#df_indexed.show()

# One-Hot Encoding
from pyspark.ml.feature import OneHotEncoder

# Initialize OneHotEncoder
encoder = OneHotEncoder(inputCol="Fruit_Index", outputCol="Fruit_OneHot")

df_encoded = encoder.fit(df_indexed).transform(df_indexed)
df_encoded.show()