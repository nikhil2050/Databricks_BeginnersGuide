
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
## **_3.1 Spark_** :
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
			<td rowspan="2">HIGHER LAYER</td>
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
			<td>LOWER LAYER</td>
			<td colspan="4">Spark standalone, YARN, Apache Mesos, K8s</td>
		</tr>
	</tbody>
</table>

### SPARK CORE:
- It takes care of Scheduling tasks, Memory management, Fault recovery, Communication with storage systems, etc.
- It's also home to Spark's main programming Abstraction API called RDD or Resilient Distributed Datasets (RDD's).
- RDDs are a collection of items distributed across various compute nodes, in the cluster that can be processed in parallel.
- Spark Core provides the APIs to create and manipulate these RDD collections.

### SPARK SQL ENGINE:
- In order to optimize the workload, Spark introduced the SQL engine.
- It includes the Catalyst Optimizer, which takes care of converting a computational query to a highly efficient execution plan
- The Tungsten Project is responsible for Memory management and CPU efficiency.

### HIGHER LAYER:
- The higher level abstraction such as Spark SQL and the Dataset and the DataFrame APIs, make it easier to develop applications and also benefit from the optimizations from the SQL engine.
- So, the recommended approach to develop applications in Spark, is to use these higher level APIs rather than the RDD API.
- The Dataset and the DataFrame APIs can be invoked from any of the domain specific languages such as Scala, Python, Java, or R.
- On top of this, we have the set of libraries such as Spark Structured Streaming for streaming, ML for MachineLearning and also GraphX for graph processing.

### LOWER LAYER:
- Spark comes with its standalone Resource manager, but you can choose other resource managers such as YARN, Apache Mesos and Kubernetes.

- Combining all of these, Spark provides the unified platform for doing streaming, batch, machine learning and graph processing workloads using a single execution engine and a standard set of APIs.

## **_3.2 Databricks_** :
- Now we knows Spark is a fast execution engine with an easy to use set of higher level APIs. But, in order to work with Spark, we have to set up our own clusters, manage security, and also use third party products to write our programs. That's where Databricks comes in.
- Databricks is a company founded by the creators of Apache Spark to make it easier to work with Spark on the Cloud.

### Clusters
- In order for Spark to do its distributed computing, we need to spin up Clusters and install the software, which Databricks spin up the Clusters with a few clicks.
- You can choose the runtime, which is suitable for your needs. For example, you can choose a runtime with ML libraries, support for GPU, etc. 
- Also, you can choose from a wide range of Clusters ranging from general purpose, memory optimized, compute optimized, or GPU enabled.

### Workspace/Notebook
- It provides a Jupyter Notebook style IDE with additional capabilities to create your application. 
- You can collaborate with your other colleagues and also integrate with configuration management tools such as Git.

### Admin Controls:
- It provides administration controls that you can use to restrict or provide access to your users, to the workspace, Clusters, etc.

### Optimized Spark (5x faster)
- Databricks provides the Spark runtime, which is highly optimized for the Databricks platform and known to be up to 5x faster than the Vannila Apache Spark.

### Databases/ Tables
- With the use of high metastore, Databricks also provides the ability to create databases and tables.

### Delta Lake
- In order to provide ACID transaction capability, Databricks also comes with the Open Source project Delta Lake,

### SQL Analytics
- It provides the data analyst a SQL based analytics environment.
- This allows the analyst to explore data, create dashboards, schedule a regular refresh of the dashboard, etc

### ML Flow
- It allows us to manage the machine learning lifecycle, including experimentation, deployment, model registry, etc.

## **_3.3 Microsoft Azure_** 
- Databricks is available on all 3 Cloud platforms - AWS, GCP, Azure. But on Azure the integration is deeper.

It makes the Databricks service available on its platform as a first party service.

Features of Azure Databricks:
Integration with:
1. Azure Active Directory
2. Unified Azure portal + Unified billing
3. Data services:
   - Azure Data Lake
   - Azure Blob Storage
   - Azure Cosmos DB
   - Azure SQL DB
   - Azure Synapse
5. Messaging service
   - Azure IoT Hub
   - Azure Event Hub
6. Power BI
7. Azure ML
8. Azure Data Factory
9. Azure Dev Ops

# Section 4: Databricks Clusters

## 4.1 Cluster Types
<table>
	<tbody>
		<tr>
			<td>ALL PURPOSE</td>
			<td>JOB CLUSTER</td>
		</tr>
		<tr>
			<td>Created Manually from GUI or API</td>
			<td>Created when Job starts to execute and the job has been configured to use a Job Cluster.</td>
		</tr>
		<tr>
			<td>They are persistent and can be terminated and restarted at any point in time.</td>
			<td>They are terminated at the end of the job. They cannot be restarted. 
So, they're no longer usable once the job has been completed.</td>
		</tr>
		<tr>
			<td>They are suitable for interactive and ad-hoc Analysis workloads.</td>
			<td>They are suitable for automated workloads, such as running an ETL pipeline</td>
		</tr>
		<tr>
			<td>They can be shared among many users, and they are good for collaborative analysis.</td>
			<td>They are isolated just for the job being executed.</td>
		</tr>
		<tr>
			<td>They are expensive to run compared to the Job Clusters.</td>
			<td>They are cheaper and great for repeated production workloads.</td>
		</tr>
		<tr>
			<td>They are great for interactive analysis and ad-hoc work.</td>
			<td>They are great for repeated production workloads.</td>
		</tr>
	</tbody>
</table>

## 4.2 All-purpose Cluster Configuration
### 4.2.1 Single/Multi node:
Single Node
- Has only 1 Driver node.
- Not suitable for large ETL loads

Multinode Node
- Has 1 Driver node, and 1/more Worker nodes 
- Driver node will distribute tasks to run on worker nodes in parallel and return the result.
- They are horizontally scalable.

### 4.2.2 Access Mode:
1. Single user
...
2. Shared
...
3. No isolation shared
...
4. Custom
...

### 4.2.3 Runtime:
Create table for:
1. Databricks Runtime
...
2. Databricks Runtime ML
...
3. Photon Runtime
...
4. Databricks Runtime Light
...

### 4.2.4 Auto-termination:
- Define inactivity time
- Default value for single-node and std. clusters is 120mins
- Value between 10 to 10000mins

### 4.2.5 Auto-Scaling:
- Specify min, max worker nodes
- Auto-scales between min and max worker nodes
- Not recommended for streaming workloads

### 4.2.6 Cluster VM Type/ Size:
1. Memory Optimzied
2. Compute  Optimzied
3. Storage  Optimzied
4. General  Optimzied
5. GPU  Optimzied

### 4.2.7 Cluster Policy
- Simplifies UI
- Enables std users to create clusters
- Achieves cost control
- Available on Premium tier only

### 4.2.8 Cluster Pool:
- It is a set of idle, ready to use VMs, that allow us to reduce the cluster start and Auto-scaling times.
- You create a pool first and then cretae the "All-purpose Compute". Now the new Compute will be created in less time.

# Section 5: Databricks Notebooks
Notebook is a collection of cells that run commands on a Databricks Cluster.

NOTE: First check if the cluster is UP and RUNNING.

> Goto Workspace
> > Users (Select a user)
> > > Create a Folder (db-course)
> > > > Create a Notebook (Name:Notebook Intro, Default Lang:Python, Cluster:db-sourse-cluster) 

## Magic Commands

Write code in Notebook:
```
message = 'Hello World'
print(message)
```
```
%sql
SELECT "Hello"
```
```
%scala
val msg = "Hello Scala"
print(msg)
```
```
%fs
ls
```
```
%sh
ps
```

## Databricks Utilities

...

1. File System Utilities
...
2. Secrets Utilities
...
3. Widget Utilities
...
4. Notebook Workflow Utilities
...

> Goto Workspace
> > Users (Select a user)
> > > Create a Folder (db-course)
> > > > Create a Notebook (Name:Databricks Utilities, Default Lang:Python, Cluster:db-sourse-cluster) 

```
dbutils.fs.ls('/databricks-datasets/COVID')   # Returns Python List
for files in dbutils.fs.ls('/databricks-datasets/COVID')
    print(files)
```
```
dbutils.help()
```
```
dbutils.fs.help()
```
```
dbutils.fs.help('ls')
```

# Section 6: Accessing Azure Data Lake from Databricks

## 6.1 Access Type:

### 6.1.1 Storage Access Keys
- Each Azure storage account comes with an Access Key, that we can use to access the storage account.

### 6.1.2 Shared Access Signature (SAS Token)
- We can generate a special kind of key called "Shared Access Signature" (SAS Token), and we can use that to access the storage account.
- It lets us manage access at a more granular level than the Access Key.

### 6.1.3 Service Principal
- We can also create a Service Principal and give the required access for the Data Lake and use those credentials to access the storage account.

## 6.2 Authentication Scope:

### 6.2.1 Session-scoped Authentication (Attached to Notebook):
- The authentication in this scenario will be valid just for the duration of the session, i.e. until the notebook has been detached to the cluster.

### 6.2.2 Cluster-scoped Authentication (Attached to Cluster):
- The authentication will happen when the Cluster starts and it will be valid until the Cluster has been terminated.
- All the notebooks connected to this Cluster will have access to the data.

### 6.2.3 Azure Active Directory(AAD) Passthrough Authentication [PREMIUM]:
- In this pattern we just need to enable the Cluster to use, Azure Active Directory Pass-through authentication.
- Whenever a user runs a Notebook, the Cluster will use the user's Azure Active Directory credentials and look for the roles, the user has been assigned to the Azure Data Lake Storage using IAM or Identity and Access management.
- If the user has access to the storage account, it will allow the user to access the storage account. Else, the user won't be able to access the storage.
- AAD Pass-through authentication is only available on premium workspaces.

### 6.2.4 Unity Catalog [PREMIUM]:
...







