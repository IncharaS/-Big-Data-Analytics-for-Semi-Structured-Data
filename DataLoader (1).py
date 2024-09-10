from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, row_number
from pyspark.sql.window import Window


def read_json_files_from_hdfs(spark, hdfs_dir):
    # Read all JSON files from the specified HDFS directory
    return spark.read.option("multiline", "true").json(hdfs_dir)


def create_policy_df(df):
    policy_df = df.select("policy_number", "coverage.plan", "coverage.effective_date", "coverage.expiry_date")
    policy_df = policy_df.withColumn("policy_id", row_number().over(policy_window))
    return policy_df


def create_member_df(df, policy_df):
    member_df = df.select("policy_number", "policy_holder.name", "policy_holder.dob", "policy_holder.gender",
                          "policy_holder.address.street", "policy_holder.address.city",
                          "policy_holder.address.state", "policy_holder.address.zipcode")
    member_df = member_df.join(policy_df.select("policy_number", "policy_id"), "policy_number", "left")
    member_df = member_df.withColumn("member_id", row_number().over(policy_window)).drop("policy_holder")
    return member_df


def create_dependent_df(df, member_df):
    dependent_df = df.select("policy_number", explode("dependents").alias("dependents"))
    for col_name in dependent_df.select("dependents.*").columns:
        dependent_df = dependent_df.withColumn(col_name, col("dependents").getItem(col_name))
    dependent_df = dependent_df.join(member_df.select("policy_number", "member_id"), ["policy_number"], "left")
    dependent_df = dependent_df.withColumn("dependent_id", row_number().over(policy_window)).drop("dependents")
    return dependent_df


def create_coverage_df(df, policy_df):
    coverage_df = df.select("policy_number", "coverage.deductible", "coverage.max_coverage_amount",
                            explode("coverage.copay").alias("copay"))
    coverage_df = coverage_df.selectExpr("*", "copay.type as copay_type", "copay.amount as copay_amount").drop("copay")
    coverage_df = coverage_df.join(policy_df.select("policy_number", "policy_id"), "policy_number", "left")
    coverage_df = coverage_df.withColumn("coverage_id", row_number().over(policy_window))
    return coverage_df


def create_claims_df(df, policy_df):
    # Explode the claims column to create separate rows for each claim
    claims_df = df.select("policy_number", explode("claims").alias("claims"))

    # Extract the keys and values from the claims column
    for col_name in claims_df.select("claims.*").columns:
        claims_df = claims_df.withColumn(col_name, col("claims").getItem(col_name))

    # Flatten the diagnosis and procedure arrays
    claims_df = claims_df.withColumn("diagnosis", explode("diagnosis")).withColumn("procedure", explode("procedure")).drop("claims")

    claims_df = claims_df.join(policy_df.select("policy_number", "policy_id"), "policy_number", "left")
    # Add an auto-generated claim ID field
    claims_df = claims_df.withColumn("claim_id", row_number().over(policy_window))

    return claims_df


def create_provider_df(df):
    provider_df = df.select("policy_number", "claim_id", "provider")
    provider_df = provider_df.withColumn("provider_id", row_number().over(policy_window))
    return provider_df


def create_diagnosis_df(df):
    diagnosis_df = df.select("policy_number", "claim_id", "diagnosis")
    diagnosis_df = diagnosis_df.withColumn("diagnosis_code", row_number().over(policy_window))
    return diagnosis_df


def create_procedure_df(df):
    procedure_df = df.select("policy_number", "claim_id", "procedure")
    procedure_df = procedure_df.withColumn("procedure_code", row_number().over(policy_window))
    return procedure_df


# Create a SparkSession
spark = SparkSession.builder \
    .appName("ECC Project") \
    .enableHiveSupport() \
    .getOrCreate()
    
policy_window = Window.orderBy(col("policy_number").asc())

# Define the HDFS directory path
hdfs_dir_path = "hdfs://localhost:9000/sample_files"

# Read all JSON files from the HDFS directory into a DataFrame
df = read_json_files_from_hdfs(spark, hdfs_dir_path)

# Create DataFrames for different entities
policy_df = create_policy_df(df)
member_df = create_member_df(df, policy_df)
dependent_df = create_dependent_df(df, member_df)
coverage_df = create_coverage_df(df, policy_df)
claims_df = create_claims_df(df, policy_df)
provider_df = create_provider_df(claims_df)
diagnosis_df = create_diagnosis_df(claims_df)
procedure_df = create_procedure_df(claims_df)

claims_df = claims_df.join(procedure_df, ["policy_number", "claim_id"], "left")
claims_df = claims_df.join(diagnosis_df, ["policy_number", "claim_id"], "left")
claims_df = claims_df.join(provider_df, ["policy_number", "claim_id"], "left")

claims_df = claims_df.select("policy_number", "claim_id", "claim_number", "policy_id", "date_of_service", "provider_id", "procedure_code", "diagnosis_code", "billed_amount", "covered_amount", "patient_responsibility")

# Output or perform further operations as needed
policy_df.show()
member_df.show()
dependent_df.show()
coverage_df.show()
claims_df.show()
provider_df.show()
diagnosis_df.show()
procedure_df.show()

# Define the Hive database and tables
database_name = "ecc_project"
policy_table = "policy"
member_table = "member"
dependent_table = "dependent"
coverage_table = "coverage"
claims_table = "claims"
provider_table = "provider"
diagnosis_table = "diagnosis"
procedure_table = "procedure"

# Define the HDFS directory where the data will be stored
hdfs_directory = "/user/hive/warehouse/insurance_data"

# Write DataFrames to Hive tables
policy_df.write.mode("overwrite").saveAsTable(f"{database_name}.{policy_table}")
member_df.write.mode("overwrite").saveAsTable(f"{database_name}.{member_table}")
dependent_df.write.mode("overwrite").saveAsTable(f"{database_name}.{dependent_table}")
coverage_df.write.mode("overwrite").saveAsTable(f"{database_name}.{coverage_table}")
claims_df.write.mode("overwrite").saveAsTable(f"{database_name}.{claims_table}")
provider_df.write.mode("overwrite").saveAsTable(f"{database_name}.{provider_table}")
diagnosis_df.write.mode("overwrite").saveAsTable(f"{database_name}.{diagnosis_table}")
procedure_df.write.mode("overwrite").saveAsTable(f"{database_name}.{procedure_table}")


# Stop the SparkSession
spark.stop()