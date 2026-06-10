import json
import boto3
import os
from datetime import datetime
import logging

from pyathena import connect
from pyathena.error import OperationalError

import pandas as pd
import io

from superset.common.chart_data import ChartDataResultFormat
from botocore.config import Config

logger = logging.getLogger()
s3_client = boto3.client('s3')
REGION = os.getenv("SUPERSET_REGION")

def generate_presigned_url(output_location: str, output_format: ChartDataResultFormat) -> str:
    s3_client = boto3.client('s3')
    bucket_name, key = output_location.replace("s3://", "").split("/", 1)
    if not isinstance(output_format, ChartDataResultFormat):
         output_format = ChartDataResultFormat.CSV
 
    file_extension = output_format.value
    timestamp = datetime.now().strftime("%Y%m%d__%H%M%S")
    filename = f"{timestamp}.{file_extension}"
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': key, 'ResponseContentDisposition': f'attachment; filename={filename}'},
        ExpiresIn=3600
    )
    return presigned_url

def run_query_and_get_s3_url(query: str) -> str:
    WORKGROUP = os.getenv("SUPERSET_WORKGROUP")
    DATABASE = os.getenv("SUPERSET_ATHENA_DB")
    S3_STAGING_DIR = os.getenv("SUPERSET_S3_STAGING_DIR")

    cursor = connect(
        s3_staging_dir=S3_STAGING_DIR,
        region_name=REGION,
        work_group=WORKGROUP,
        schema_name=DATABASE,
    ).cursor()

    cursor.execute(query)

    return cursor.output_location

def transform_csv_to_xlsx(csv_location: str):
     
    config = Config(
        region_name=REGION,
        read_timeout=900
    )
    lambda_client = boto3.client("lambda", config=config)

    lambda_response = lambda_client.invoke(
        FunctionName=os.getenv("SUPERSET_EXCEL_LAMBDA"),
        InvocationType='RequestResponse',
        Payload=bytes(json.dumps({"csv_location": csv_location}), 'utf-8')
    )

    response_payload = json.loads(lambda_response["Payload"].read().decode("utf-8"))
    return response_payload["xlsx_s3_path"]
