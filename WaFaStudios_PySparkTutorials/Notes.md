### Video 01: What is PySpark?
- PySpark is an interface for Apache Spark in Python. PySpark supports most of Spark's features such as 
Spark SQL, DataFrame, Streaming, MLlib and Spark Core.

### Video 02: Created Dataframe manually with hard coded values in Pyspark
- Use createDataFrame() from Spark Session object
- DataFrame is a distributed collection of data organized into named columns. It is conceptually equivalent to a table is relational database.


### Video 03: Read CSV file into DataFrame using Pyspark
- Using *csv("path")* or *format("csv").load("path")* of DataFrameReader, you can read a CSV file into a Pyspark DataFrame.


### Video 04: Write DataFrame into CSV using Pyspark


### Video 09: show()
- Only gives 20 characters in a column.
- using truncate we can able to see entire column values.


### Video 10: withColumn()
- PySpark withColumn() is a transformation function of DataFrame which is used to change the value, convert the datatype of an existing column, create a new column, and many more.


### Video 11: withColumnRenamed()
- Rename column in DataFrame

### Video 12: StructType() & StructField()
- PySpark StructType & StructField are used to programmatically specify the schema to the DataFrame and create complex columns like nested struct, array, and map columns.
- StructType is a collection of StructField's.

### Video 13: ArrayType Columns in PySpark

### Video 14: explode(), split(), array() and array_contains() Functions is PySpark

- explode(): To create a new row for each element in the given array column.
- split(): Returns an array type after splitting the string column by delimiter.
- array(): Use array() function to create a new array column by merging the data from multiple columns.
- array_contains(): Used to check if array column contains a value. Returns null if the array is null, true if the array contains the value, and false otherwise.

### Video 15: MapType Column in PySpark
- PySpark MapType is used to represent map key-value pair similar to python dictionary(dict)

### Video 16: map_keys(), map_values() & explode() functions to work with MapType columns in Pyspark

### Video 17: Row Class in PySpark
- pyspark.sql.Row which is represented as a record/raw in DataFrame, one can create a Row object by using named arguments or create a custom Row like class.

### Video 18: Column Class in PySpark
- PySpark Column class represents a single Column in a DataFrame.
- pyspark.sql.Column class provides several functions to work with DataFrame to manipulate the Column values, evaluate the boolean expression to filter rows, retrieve a value or part of a value from a DataFrame column.
- One of the simplest ways to create a column class object is by using PySpark lit() SQL function.

### Video 19: When() & Otherwise() Functions in PySpark
- It's similar to SQL Case When, executes sequence of expressions until it matches the condition and returns a value when match.

### Video 20: alias(), asc(), desc(), cast() & like() functions on columns of dataframe in PySpark

### Video 21: filter() & where() in PySpark
- PySpark filter() function is used to filter the rows from DataFrame based on the given condition or SQL expression.
- You can also use where() clause instead of the filter() if you are coming from SQL background, both these functions operate exactly the same.

### Video 22: distinct() & dropDuplicates() in PySpark
- PySpark distinct() function is used to remove the duplicate rows(all columns)
- dropDuplicates is used to drop rows based on selected (one or multiple) columns.
- So basically, using these functions we can get distinct rows.

### Video 23: orderBy() & sort() in Pyspark
- You can use either sort() or OrderBy() functions of PySpark DataFrame to sort DataFrame by ascending or descending order based on single or multiple columns.
- By default, sorting will happen in ascending order. We can explicitly mention ascending or descending using asc(), desc() functions.

### Video 24: Union() & UnionAll() in PySpark
- union() and unionAll() transformations are used to merge two or more DataFrames of the same schema or structure.
- union() & unionAll() method merges two DataFrames and returns the new DataFrame with all rows from two DataFrames regardless of duplicate data.
- To remove duplicates use distinct() function


### Video 25: groupBy() in PySpark
- Similar to SQL GROUP BY claus, PySpark groupBy() function is used to collect the identical data into groups on DataFrame and perform count, sum, avg, min, max functions on the grouped data.

### Video 26: GroupBy() agg() function in PySpark
- PySaprk GroupBy agg() is used to calculate more than one aggregate (multiple aggregates) at a time on grouped DataFrame.

### Video 27: unionByName() function in PySpark
- unionByName() lets you to merge/union two DataFrames with a different number of columns (different schema) by passing allowMissingColumns with the value true.

### Video 28: select() function in PySpark
- select() function is used to select single, multiple column by index, all columns from the list and the nested columns from a DataFrame.

### Video 29 & 30: join() function in PySpark
- join() is like SQL JOIN. We can combine columns from different DataFrames based on condition. It supports all basic join types such as INNER, LEFT, OUTER, RIGHT OUTER, LEFT ANTI, LEFT SEMI, CROSS, SELF

### Video 31: pivot() function in PySpark
- Used to rotate data in one column into multiple columns.
- It's an aggregation where one of the grouping column values will be converted in individual columns.

### Video 32: unpivot() function in PySpark
- Unpivot is rotating columns into rows, PySpark SQL doesn't have unpivot function hence will use the stack() function


### Video 33: fill() & fillna() functions in PySpark
- fillna() or DataFrameNaFunctions.fill() is used to replace NULL/None values on all or selected multiple DataFrame columns with either zero(0), empty string, space, or any constant literal values.

### Video 34: sample() function in PySpark
- To get the random sampling subset from the large dataset.
- Use fraction to indicate what percentage of data to return and seed values to make sure every time to get same random sample.

### Video 35: collect()
- collect() retrieves all elements in a DataFrame as an Array of Row type to the driver node.
- collect() is an action hence it does not return a DataFrame instead, it returns data in an Array to the driver. Once the data is in an array, you can use python for loop to process it further.
- collect() use it with small DataFrames. With big dataFrames it may result in out of memory error as its return entire data to single node(driver)

### Video 36: DataFrame.transform() function in PySpark
- It's used to chain the custom transformations and this function returns the new DataFrame after applying the specified transformations.

### Video 37: pyspark.sql.functions.transform() function in PySpark
- It's used to apply the transformation on a column of type Array. This function applied the specified transformation on every element of the array and returns an object of ArrayType.

### Video 38: createOrReplaceTempView()
- Advantage of Spark, you can work with SQL along with DataFrames. That means, if you are comfortable with SQL, you can create temporary view on DataFrame by using createOrReplaceTempView() and use SQL to select and manipulate data.
- Temp views are session scoped and cannot be shared between the sessions.

### Video 39: createOrReplaceGlovalTempView()
- It's used to create temp views or tables globally, when can be accessed across the sessions within Spark Application.
- To query these tables, we need append global_temp (table_name)
    
### Video 40: UDF in PySpark
- These are similar to functions in SQL. We define some logic in functions and store them in Database and use them in queries.
- Similar to that we can write our own custom logic in python function and register it with PySpark using udf() function.
- In order to use with spark sql, you need to register with spark.udf.register()

### Video 41: Convert RDD to DataFrame
- RDD, its collection of objects similar to list in Python. Its Immutable & In memory processing.
- By using parallelize() function of SparkContext you can create an RDD.

### Video 42: map() transformation in PySpark
- Its RDD transformation used to apply function (lambda) on every element of RDD and returns new RDD.
- Dataframe doesn't have map() transformation to use with Dataframe you need to generate RDD first.

### Video 43: flatMap() transformation in PySpark
- flatMap() is a transformation operation that flattens the RDD (array/map DataFrame columns) after applying the function on every element and returns a new PySpark RDD.
- Its not available in dataframes. Explode() functions can be used in dataframes to flatten arrays.

### Video 44: partitionBy function in PySpark
- Its used to partition large Dataset into smaller files based on one or multiple columns.
