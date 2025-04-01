import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1743447543987 = glueContext.create_dynamic_frame.from_catalog(database="stedi-analytics", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1743447543987")

# Script generated for node Customer Trusted
CustomerTrusted_node1743448125353 = glueContext.create_dynamic_frame.from_catalog(database="stedi-analytics", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1743448125353")

# Script generated for node Join
Join_node1743448164836 = Join.apply(frame1=AccelerometerLanding_node1743447543987, frame2=CustomerTrusted_node1743448125353, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1743448164836")

# Script generated for node SQL Query
SqlQuery0 = '''
select user, x, y, z
from myDataSource
'''
SQLQuery_node1743448258830 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1743448164836}, transformation_ctx = "SQLQuery_node1743448258830")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1743448258830, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743447263748", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1743448334820 = glueContext.getSink(path="s3://psp-stedi-bucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1743448334820")
AccelerometerTrusted_node1743448334820.setCatalogInfo(catalogDatabase="stedi-analytics",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1743448334820.setFormat("json")
AccelerometerTrusted_node1743448334820.writeFrame(SQLQuery_node1743448258830)
job.commit()