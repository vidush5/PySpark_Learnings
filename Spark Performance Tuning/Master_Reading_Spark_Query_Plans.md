# Spark's Query Plan

Image Credit : Databricks

<p align="center">
  <a href="https://example.com">
    <img src="https://github.com/vidush5/PySpark_Learnings/blob/master/Spark%20Performance%20Tuning/sparkqueryplan.png" alt="Example Image" />
  </a>
</p>

In the first step of a Spark query plan, Spark checks for syntax errors to ensure that the query is valid. Once the syntax is validated, a logical plan is developed using the catalog. This logical plan is then refined through a process called logical optimization, which helps to create an optimized logical plan. After logical optimization, Spark generates a physical plan based on cost considerations. The final physical plan is selected, and corresponding RDD (Resilient Distributed Dataset) code is developed. During the optimization phase, techniques such as filter pushdown and project pushdown are employed to enhance performance and efficiency. These optimizations help in reducing data shuffling and minimizing the amount of data processed, ultimately leading to faster query execution.

### Narrow Transformation
- filter rows where city='boston'
- add a new column: adding first_name and last_name
- alter an existing column: adding 5 to age column
- select relevant columns

### Wide Transformation
- Repartition: Used to both Increase & Decrease number of partitions.
- Coalesce: Helps you to reduce the partitions 
- GroupBy
  */ count
  */ countDistinct
  */ sum

