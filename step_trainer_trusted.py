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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1790211700132 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalog_node1790211700132")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1790211970994 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="AWSGlueDataCatalog_node1790211970994")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT s.*
FROM step_trainer s
WHERE EXISTS (
    SELECT 1
    FROM customers c
    WHERE s.serialnumber = c.serialnumber
)
'''
SQLQuery_node1790212055194 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customers":AWSGlueDataCatalog_node1790211970994, "step_trainer":AWSGlueDataCatalog_node1790211700132}, transformation_ctx = "SQLQuery_node1790212055194")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1790212055194, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790211551370", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1790212469231 = glueContext.getSink(path="s3://stedi-d609/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1790212469231")
AmazonS3_node1790212469231.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1790212469231.setFormat("glueparquet", compression="snappy")
AmazonS3_node1790212469231.writeFrame(SQLQuery_node1790212055194)
job.commit()