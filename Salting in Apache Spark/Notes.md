### Data Skewness (Salting)
It occurs when data is unevenly distributed across partitions, resulting in some partitions having to process much more data than others. This disparity can create performance bottlenecks, ultimately slowing down the overall execution of the job.

#### Causes of Data Skewness
- GroupBy Operations
- Skewed Data Distribution
- Inadequate Partitioning Strategy
- Join Operations

#### Data Skewness Cons
- Slow-Running Stages/Tasks
- Spilling Data to Disk
