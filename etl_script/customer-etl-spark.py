import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

# Spark 및 GlueContext 초기화
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Glue Catalog에서 두 테이블 로드
user_info_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="<SourceDatabase>",
    table_name="<SourceTable1>"
)

add_info_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="<SourceDatabase>",
    table_name="<SourceTable2>"
)

# DynamicFrame → DataFrame으로 변환
user_info_df = user_info_dyf.toDF()
add_info_df = add_info_dyf.toDF()

# 두 DataFrame을 id 기준 inner join
joined_df = user_info_df.join(add_info_df, on="id", how="inner")

# 다시 DynamicFrame으로 변환
joined_dyf = DynamicFrame.fromDF(joined_df, glueContext, "joined_dyf")

# 병합된 데이터를 Parquet 형식으로 S3에 저장
glueContext.write_dynamic_frame.from_options(
    frame=joined_dyf,
    connection_type="s3",
    connection_options={"path": "s3://<DestinationBucket>/<DestinationPath>/"},
    format="parquet"
)