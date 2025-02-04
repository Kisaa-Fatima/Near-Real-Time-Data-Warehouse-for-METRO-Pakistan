# Near-Real-Time-Data-Warehouse-for-METRO-Pakistan
Designed and implemented a near-real-time data warehouse prototype for METRO Pakistan using Java and Eclipse, leveraging the extended MESHJOIN algorithm for efficient ETL. Developed a star schema for multidimensional data analysis and executed advanced OLAP queries for revenue trends, product affinity, and seasonal sales analysis.

1.Set Up the Environment:

Download the eclipse folder and run eclipse.exe file.
Setup eclipse and create java project, import mysql connector as external jar file.
Install MySQL server and workbench, set it up and create a database named proj.

2.Establish connection and load data from csv.
Establish a connection between eclipse and mysql, test the connection.
Import the required CSV files from eclipse into mysql schema. Data will be loaded inside the transactions_fact, customer_dim, and product_data tables using the provided SQL script.

3.Perform meshjoin and run the Steps Sequentially:

Step 1: Load transaction stream segments using Step1TransactionStream.java.
Step 2: Load complete dimension data (customer_dim and product_data) into memory using Step2LoadMDPartitions.java.
Step 3: Join transaction stream with dimension data using Step4EnrichTransaction.java.
Step 4: Calculate derived fields like sale and load enriched data into dw_transaction using Step5LoadData.java.
Run the Mesh Join:

Execute the meshjoin.java file to automate the above steps iteratively until all transactional data is loaded into dw_transaction.

4. Verify Data in the Data Warehouse:
Use SQL queries to validate the correctness of the loaded data. Once verified, perform OLAP queries

Troubleshooting:
If the data does not load as expected, check:
Database connection settings in Java files.
Input CSV files for correctness.
Logs printed by the Java programs for errors.
(UPLOADING THE ZIP FILE DOWNLOAD THAT AND RUN THAT, OVER HERE JUST UPLOADING THE CODE FILE)
