import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node daily_raw_data_from_s3
daily_raw_data_from_s3_node1708315211999 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="airlines",
        table_name="raw",
        transformation_ctx="daily_raw_data_from_s3_node1708315211999",
    )
)

# Script generated for node dimension_airport_code_read
dimension_airport_code_read_node1708315329228 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="airlines",
        table_name="dev_airlines_airports_dim",
        transformation_ctx="dimension_airport_code_read_node1708315329228",
    )
)

# Script generated for node Join
Join_node1708316285649 = Join.apply(
    frame1=daily_raw_data_from_s3_node1708315211999,
    frame2=dimension_airport_code_read_node1708315329228,
    keys1=["originairportid"],
    keys2=["airport_id"],
    transformation_ctx="Join_node1708316285649",
)

# Script generated for node dept_airport_schema
dept_airport_schema_node1708316399898 = ApplyMapping.apply(
    frame=Join_node1708316285649,
    mappings=[
        ("carrier", "string", "carrier", "string"),
        ("destairportid", "long", "destairportid", "long"),
        ("depdelay", "long", "dep_delay", "bigint"),
        ("arrdelay", "long", "arr_delay", "bigint"),
        ("city", "string", "dep_city", "string"),
        ("name", "string", "dep_airport", "string"),
        ("state", "string", "dep_state", "string"),
    ],
    transformation_ctx="dept_airport_schema_node1708316399898",
)

# Script generated for node Join
Join_node1708316675554 = Join.apply(
    frame1=dept_airport_schema_node1708316399898,
    frame2=dimension_airport_code_read_node1708315329228,
    keys1=["destairportid"],
    keys2=["airport_id"],
    transformation_ctx="Join_node1708316675554",
)

# Script generated for node Change Schema
ChangeSchema_node1708316745092 = ApplyMapping.apply(
    frame=Join_node1708316675554,
    mappings=[
        ("carrier", "string", "carrier", "string"),
        ("destairportid", "long", "destairportid", "long"),
        ("dep_delay", "bigint", "dep_delay", "long"),
        ("arr_delay", "bigint", "arr_delay", "long"),
        ("dep_city", "string", "dep_city", "string"),
        ("dep_airport", "string", "dep_airport", "string"),
        ("dep_state", "string", "dep_state", "string"),
        ("airport_id", "long", "airport_id", "long"),
        ("city", "string", "arr_city", "string"),
        ("name", "string", "arr_airport", "string"),
        ("state", "string", "arr_state", "string"),
    ],
    transformation_ctx="ChangeSchema_node1708316745092",
)

# Script generated for node redshift_fact_table_write
redshift_fact_table_write_node1708316826166 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=ChangeSchema_node1708316745092,
        database="airlines",
        table_name="dev_airlines_daily_flights_fact",
        redshift_tmp_dir="s3://temp-airline",
        additional_options={
            "aws_iam_role": "arn:aws:iam::701389125354:role/redshift-role"
        },
        transformation_ctx="redshift_fact_table_write_node1708316826166",
    )
)

job.commit()
