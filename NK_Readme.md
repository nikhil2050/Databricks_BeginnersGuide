
Reference: https://adp-gptlearning.udemy.com/course/azure-databricks-spark-core-for-data-engineers/learn/lecture/27514596#overview

# What we are doing?

Build a DataLake in Azure by:
1. ingesting Formula1 data(Ingested Layer) from external API(Raw Layer).
2. transforming the data as required for Reporting and Analysis(Presenting Layer).
3. a. Analyze the data for trends using Databricks and create the necessary dashboards.\
   b. Make the data available for BI Reporting and demonstrate how to access this data from PowerBI.

All of this process will be scheduled and triggered by Azure Data.
Factory pipelines will then go through the new and emerging Data Lakehouse Architecture and convert a solution to create, a DataLake House using Delta Lake.

| **Overviews** | **Databricks** | **Spark (Python)** | **Spark (SQL)** | **DeltaLake** | Orchestration |
|---|---|---|---|---|---|
| Azure portal | Clusters | Data Ingestion 1 | Temp Views | Delta Lake Architectures | Azure Data Factory |
| Azure Databricks | Notebooks  | Data Ingestion 2 | DDL |   | Connecting other Tools |
| Project Overview | DeltaLake Access | Data Ingestion 3 | DML |   | |
| Spark Overview | Securing Access  | Transformation | Analysis |   | |
| | Databricks Mounts  | Aggregations | Incremental Load (PySpark+PySQL) |   |  |
| | Jobs  | Incremental Load |   |   |  |

# Section 3: "Azure Databricks" Overview
1. **_Spark_** :
It is the open source distributed Compute-processing engine for developing big data projects. It is at the core of Azure Databricks.
Apache Spark is a lightning fast, unified analytics engine for big data processing and machine learning.
3. **_Databricks_** = Company created by Spark founders
4. **_Microsoft Azure_** = Makes the Databricks service available on its platform as a first party service.
These three offerings together makes Azure Databricks.
