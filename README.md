# Big-Data-Analytics-for-Semi-Structured-Data
The project aims to integrate semi-structured data into the Hadoop ecosystem utilizing Apache 
Hadoop and Apache Spark technologies. By leveraging the scalability and processing power of 
Hadoop, we aim to efficiently manage and analyze large volumes of semi-structured data, 
ultimately creating a structured table in Apache for further analysis and reporting

The proposed design is executed as follows:


Step 1: Generate JSON Files
fileGenerator.py generates random JSON files with simulated data structured similar to health 
insurance policies, with fields like policy_number, policy_holder, dependents, claims, 
billed_amount, claimed_amount, and more relevent fields. These files are crucial for testing and 
analyzing the data processing pipeline. We created around 10000 sample files for this project.

Step 2: Store Files in HDFS
Generated JSON files are stored in HDFS.
 hdfs dfs –put sample_files/

 
Step 3: Dataloader.py 
This file configures and starts a Spark session on the Jetstream machine. Using this session read 
all the data files generated earlier into dataframes.
Flatten this dataframes for different entities, and perform join operations to maintain parent-child 
relationship between different dataframes.
Define the Hive database and tables, and write these dataframes to Hive.
To run the DataLoader.py script using Spark.
 spark-submit --master spark://10.0.195.33:7077 DataLoader.py
This command uses spark-submit to run your DataLoader.py script. 
 - --master spark://10.0.195.33:7077: Specifies the Spark master node’s address and port. This 
directs Spark to execute the job on the cluster managed by the master at the specified IP address.


Step 4 : Use spark-sql to run Analytics queries.
Once the data is loaded and processed into Spark, one can use Spark SQL to run various analytical 
queries to extract insights from the data, enabling deeper insights and data-driven decision-making 
in your healthcare insurance analysis.
