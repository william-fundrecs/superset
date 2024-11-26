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
    