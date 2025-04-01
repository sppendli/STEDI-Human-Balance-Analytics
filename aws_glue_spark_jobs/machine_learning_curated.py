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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1743450428839 = glueContext.create_dynamic_frame.from_catalog(database="stedi-analytics", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1743450428839")

# Script generated for node Customer Trusted
CustomerTrusted_node1743450630701 = glueContext.create_dynamic_frame.from_catalog(database="stedi-analytics", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1743450630701")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1743450596790 = glueContext.create_dynamic_frame.from_catalog(database="stedi-analytics", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1743450596790")

# Script generated for node Accelerometer Trusted
SqlQuery1 = '''
select user, timestamp, x, y, z
from ac
inner join ct
    on ac.user = ct.email
'''
AccelerometerTrusted_node1743451021776 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"ac":AccelerometerLanding_node1743450596790, "ct":CustomerTrusted_node1743450630701}, transformation_ctx = "AccelerometerTrusted_node1743451021776")

# Script generated for node SQL Query
SqlQuery0 = '''
select 
    st.sensorreadingtime, 
    ac.x as x, 
    ac.y as y, 
    ac.z as z
from st
inner join ac
    on st.sensorreadingtime = ac.timestamp
'''
SQLQuery_node1743450664009 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"st":StepTrainerTrusted_node1743450428839, "ac":AccelerometerTrusted_node1743451021776}, transformation_ctx = "SQLQuery_node1743450664009")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1743450664009, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743450480600", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1743451858931 = glueContext.getSink(path="s3://psp-stedi-bucket/machinelearning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1743451858931")
MachineLearningCurated_node1743451858931.setCatalogInfo(catalogDatabase="stedi-analytics",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1743451858931.setFormat("json")
MachineLearningCurated_node1743451858931.writeFrame(SQLQuery_node1743450664009)
job.commit()