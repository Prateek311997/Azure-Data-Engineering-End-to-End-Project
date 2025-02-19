# Azure-Data-Engineering-End-to-End-Project

## **Abstract**
This project demonstrates an end-to-end data engineering pipeline utilizing Azure Data Factory, Databricks, PySpark, and Azure Synapse Analytics. The project focuses on ingesting data from APIs, processing and transforming data using Databricks and PySpark, and storing it in a structured data warehouse. The final data is then made available for analytics and visualization. This document provides an in-depth overview of the architecture, technologies, and implementation details of the project.

## **1. Introduction**
Data engineering is a crucial part of any data-driven organization, ensuring the efficient movement, transformation, and storage of data. This project provides a real-world use case of building an Azure-based data engineering pipeline. The project follows the Medallion Architecture (Bronze, Silver, and Gold layers) to process and refine data for analytics.

## **2. Project Architecture**
The project follows a structured architecture comprising the following components:

- **Data Ingestion:** Using Azure Data Factory to pull data from APIs.
- **Storage:** Storing raw data in Azure Data Lake Storage Gen2 (Bronze Layer).
- **Processing:** Using Azure Databricks and PySpark to clean and transform data (Silver Layer).
- **Data Warehousing:** Storing the transformed data in Azure Synapse Analytics (Gold Layer).
- **Visualization:** Connecting Power BI to Synapse Analytics for dashboard creation.

## **3. Technologies Used**
- **Azure Data Factory** – Orchestration and ETL
- **Azure Data Lake Storage Gen2** – Data storage
- **Azure Databricks & PySpark** – Data transformation and processing
- **Azure Synapse Analytics** – Data warehousing and SQL-based analytics
- **Power BI** – Data visualization

## **4. Step-by-Step Resource Creation**

### **4.1 Creating a Resource Group**
1. Go to the [Azure Portal](https://portal.azure.com/).
2. Click on **Create a Resource** and select **Resource Group**.
3. Enter a unique **Resource Group Name**.
4. Select a **Region** close to your location.
5. Click **Review + Create** and then **Create**.

### **4.2 Creating an Azure Data Lake Storage Gen2 Account**
1. Navigate to the **Azure Portal** and click on **Create a Resource**.
2. Search for **Storage Account** and click **Create**.
3. Select your previously created **Resource Group**.
4. Enter a unique **Storage Account Name**.
5. Choose **Region** and select **Standard performance**.
6. Under **Advanced** settings, enable **Hierarchical Namespace** to support Data Lake Gen2.
7. Click **Review + Create**, then **Create**.

### **4.3 Creating Azure Data Factory**
1. In the **Azure Portal**, click on **Create a Resource**.
2. Search for **Data Factory** and click **Create**.
3. Select your **Resource Group**.
4. Enter a **Data Factory Name**.
5. Choose **Region** and other default settings.
6. Click **Review + Create**, then **Create**.

### **4.4 Creating an Azure Databricks Workspace**
1. In the **Azure Portal**, click on **Create a Resource**.
2. Search for **Azure Databricks** and click **Create**.
3. Select your **Resource Group** and enter a **Workspace Name**.
4. Choose a **Pricing Tier** (Standard/Premium).
5. Click **Review + Create**, then **Create**.

### **4.5 Creating an Azure Synapse Analytics Workspace**
1. In the **Azure Portal**, click on **Create a Resource**.
2. Search for **Azure Synapse Analytics** and click **Create**.
3. Select your **Resource Group** and enter a **Workspace Name**.
4. Choose a **Storage Account** for data storage.
5. Enable **Managed Virtual Network** for better security.
6. Click **Review + Create**, then **Create**.

### **4.6 Setting Up Power BI for Visualization**
1. Open **Power BI Desktop**.
2. Click **Get Data** and select **Azure Synapse Analytics**.
3. Enter the **SQL Server Name** from your Synapse workspace.
4. Choose the required **database and tables**.
5. Load data and start building visualizations.

## **5. Data Ingestion & Processing**
The data is ingested from a GitHub-hosted API containing structured CSV data. The ingestion process includes:
1. **Extracting data** from the API using Azure Data Factory (ADF) with an HTTP connector.
2. **Storing raw data** in the Bronze Layer (Azure Data Lake Storage Gen2).
3. **Creating linked services** in ADF for API and storage connectivity.

## **6. ETL Pipelines using Azure Data Factory**
The ETL pipeline consists of:
- **Copy Activity:** Pulls data from the API and stores it in the Bronze Layer.
- **Transformation Activity:** Uses Databricks to clean and transform data.
- **Data Movement Activity:** Loads refined data into Azure Synapse Analytics.

## **7. Data Transformation using Databricks & PySpark**
The data transformation includes:
- Removing duplicates and null values.
- Normalizing data formats.
- Aggregating relevant data fields.
- Applying business logic for analytics.

## **8. Data Warehousing in Azure Synapse Analytics**
- The transformed data is loaded into Synapse Analytics tables.
- Fact and dimension tables are created for efficient querying.
- SQL queries are used to optimize data retrieval.

## **9. Data Visualization using Power BI**
Power BI is connected to Azure Synapse Analytics for data visualization. Key steps include:
- Establishing a connection between Power BI and Synapse.
- Creating dashboards for sales and customer insights.
- Implementing dynamic filters and interactive visual elements.

## **10. Conclusion**
This project demonstrates a practical approach to implementing an Azure-based data engineering solution. By leveraging Azure Data Factory, Databricks, and Synapse Analytics, the project showcases how to build a scalable, efficient, and production-ready data pipeline.



