# Import libraries

import findspark

findspark.init()

from pyspark import SparkContext
from pyspark.sql import SparkSession, Window, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("Project_01").getOrCreate()


def load_dataframe(filename):
    df = spark.read.format('csv').options(header='true').load(filename)
    return df


# Creating a dataframe
df_matches = load_dataframe('./Data/Matches.csv')
df_matches.limit(5).show()

# Lets rename some of the columns
old_cols = df_matches.columns[-3:]
new_cols = ["HomeTeamGoals", "AwayTeamGoals", "FinalResult"]
old_new_cols = [*zip(old_cols, new_cols)]
for old_col, new_col in old_new_cols:
    df_matches = df_matches.withColumnRenamed(old_col, new_col)

df_matches.limit(5).toPandas()

