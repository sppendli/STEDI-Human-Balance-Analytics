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

# Script generated for node Customer Curated
CustomerCurated_node1743449555238 = glueContext.create_dynamic_frame.from_catalog(database="stedi-analytics", table_name="customer_curated", transformation_ctx="CustomerCurated_node1743449555238")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1743449521873 = glueContext.create_dynamic_frame.from_catalog(database="stedi-analytics", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1743449521873")

# Script generated for node SQL Query
SqlQuery0 = '''
select
    st.sensorreadingtime,
    st.serialnumber,
    st.distancefromobject
from step_trainer_landing st
inner join customer_curated cc
    on st.serialnumber = cc.serialnumber
'''
SQLQuery_node1743449780846 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":CustomerCurated_node1743449555238, "step_trainer_landing":StepTrainerLanding_node1743449521873}, transformation_ctx = "SQLQuery_node1743449780846")

# Script generated for node Steo Trainer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1743449780846, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743449588707", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
SteoTrainerTrusted_node1743450012686 = glueContext.getSink(path="s3://psp-stedi-bucket/step_training/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="SteoTrainerTrusted_node1743450012686")
SteoTrainerTrusted_node1743450012686.setCatalogInfo(catalogDatabase="stedi-analytics",catalogTableName="step_trainer_trusted")
SteoTrainerTrusted_node1743450012686.setFormat("json")
SteoTrainerTrusted_node1743450012686.writeFrame(SQLQuery_node1743449780846)
job.commit()