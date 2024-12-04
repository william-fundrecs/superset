import boto3
from datetime import datetime


def generate_presigned_url(output_location: str) -> str:
    s3_client = boto3.client('s3')
    bucket_name, key = output_location.replace("s3://", "").split("/", 1)
    timestamp = datetime.now().strftime("%Y%m%d__%H%M%S")
    filename = f"{timestamp}.csv"
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': key, 'ResponseContentDisposition': f'attachment; filename={filename}'},
        ExpiresIn=3600
    )
    return presigned_url

def run_query_and_get_s3_url(query, database):
    athena_client = boto3.client('athena')
    
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        WorkGroup='superset-etl',
    )
    
    query_execution_id = response['QueryExecutionId']
    
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
    
    if status == 'SUCCEEDED':
        return query_status['QueryExecution']['ResultConfiguration']['OutputLocation']
    else:
        raise Exception(f"Query failed with status: {status}")
    