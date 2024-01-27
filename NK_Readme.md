
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
   - It is the open source distributed Compute-processing engine for developing big data projects. It is at the core of Azure Databricks.
   - Apache Spark is a lightning fast, unified analytics engine for big data processing and machine learning.
   - It was built to address the shortcomings of Hadoop. Hadoop was slow and inefficient for interactive and iterative computing jobs, and it was too complex to learn and develop.
   - Spark offers a much simpler, faster and easier APIs to develop on.
   - Spark can be 100 X faster than Hadoop, for large scale data processing by exploiting in-memory computing and other optimizations.
   - Spark runs on a distributed computing platform.
   - Spark has an unified engine to support varying workloads.
   - It comes packaged with high level libraries, including support for SQL queries, streaming data, ML and Graph processing.
   - These standard libraries increase developer productivity and can be seamlessly combined to create complex workflows.

<table>
	<tbody>
		<tr>
			<td rowspan="2"></td>
			<td rowspan="2">Spark SQL</td>
			<td>Spark Streaming</td>
			<td>Spark ML</td>
			<td>Spark Graph</td>
		</tr>
		<tr>
			<td colspan="3">DataFrame / Dataset APIs</td>
		</tr>
		<tr>
			<td colspan="5"></td>
		</tr>
		<tr>
			<td>SPARK SQL ENGINE</td>
			<td colspan="2">Catalyst Optimizer</td>
			<td colspan="2">Tungsten</td>
		</tr>
		<tr>
			<td colspan="5"></td>
		</tr>
		<tr>
			<td rowspan="2">SPARK CORE</td>
			<td>Scala</td>
			<td>Python</td>
			<td>Java</td>
			<td>R</td>
		</tr>
		<tr>
			<td colspan="4">Resilient Distributed Dataset (RDD)</td>
		</tr>
		<tr>
			<td colspan="5"></td>
		</tr>
		<tr>
			<td colspan="5">Spark standalone, YARN, Apache Mesos, K8s</td>
		</tr>
	</tbody>
</table>

2. **_Databricks_** = Company created by Spark founders

3. **_Microsoft Azure_** = Makes the Databricks service available on its platform as a first party service.
These three offerings together makes Azure Databricks.
