
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
