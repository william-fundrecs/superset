# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
AWS utilities for Athena S3 direct-download support.

Environment variables:
  SUPERSET_REGION         — AWS region (e.g. eu-west-1)
  SUPERSET_WORKGROUP      — Athena workgroup with a pre-configured S3 output location
  SUPERSET_ATHENA_DB      — Athena database (catalog) name
  SUPERSET_S3_STAGING_DIR — S3 URI for Athena result staging
  SUPERSET_EXCEL_LAMBDA   — Lambda function name for CSV-to-XLSX conversion
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

from superset.utils.json import dumps, loads
from superset.common.chart_data import ChartDataResultFormat

logger = logging.getLogger(__name__)

_PRESIGNED_URL_EXPIRY_SECONDS = 3600  # 1 hour


def _region() -> str:
    return (
        os.environ.get("SUPERSET_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "eu-west-1"
    )


def _get_s3_client():  # type: ignore[no-untyped-def]
    import boto3  # pylint: disable=import-outside-toplevel

    return boto3.client("s3", region_name=_region())


def run_query_and_get_s3_url(sql: str) -> Optional[str]:
    """
    Submit *sql* to Athena via pyathena and return the S3 URI 
    of the result CSV, or None.
    """
    workgroup = os.environ.get("SUPERSET_WORKGROUP")
    database = os.environ.get("SUPERSET_ATHENA_DB")
    staging_dir = os.environ.get("SUPERSET_S3_STAGING_DIR")

    if not workgroup or not database:
        logger.error(
            "SUPERSET_WORKGROUP and SUPERSET_ATHENA_DB must be set"
        )
        return None

    try:
        from pyathena import connect  # pylint: disable=import-outside-toplevel

        cursor = connect(
            s3_staging_dir=staging_dir,
            region_name=_region(),
            work_group=workgroup,
            schema_name=database,
        ).cursor()
        cursor.execute(sql)
        logger.info(
            "Athena query %s output: %s", cursor.query_id, cursor.output_location
        )
        return cursor.output_location
    except Exception:  # pylint: disable=broad-except
        logger.exception("Athena query failed")
        return None


def generate_presigned_url(
    s3_uri: str, output_format: ChartDataResultFormat
) -> Optional[str]:
    """Generate a pre-signed download URL with a timestamped filename."""
    if not s3_uri or not s3_uri.startswith("s3://"):
        logger.error("Invalid S3 URI: %s", s3_uri)
        return None

    if not isinstance(output_format, ChartDataResultFormat):
        output_format = ChartDataResultFormat.CSV

    bucket, _, key = s3_uri[len("s3://") :].partition("/")
    timestamp = datetime.now().strftime("%Y%m%d__%H%M%S")
    filename = f"{timestamp}.{output_format.value}"

    try:
        url = _get_s3_client().generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket,
                "Key": key,
                "ResponseContentDisposition": f"attachment; filename={filename}",
            },
            ExpiresIn=_PRESIGNED_URL_EXPIRY_SECONDS,
        )
        return url
    except Exception:  # pylint: disable=broad-except
        logger.exception("Failed to generate presigned URL for %s", s3_uri)
        return None


def transform_csv_to_xlsx(csv_location: str) -> Optional[str]:
    """
    Invoke the XLSX Lambda to convert an Athena CSV result to XLSX, 
    returning the new S3 URI.
    """
    fn = os.environ.get("SUPERSET_EXCEL_LAMBDA")
    if not fn:
        logger.error("SUPERSET_EXCEL_LAMBDA must be set for XLSX conversion")
        return None

    try:
        import boto3  # pylint: disable=import-outside-toplevel
        from botocore.config import Config  # pylint: disable=import-outside-toplevel

        client = boto3.client(
            "lambda",
            config=Config(region_name=_region(), read_timeout=900),
        )
        response = client.invoke(
            FunctionName=fn,
            InvocationType="RequestResponse",
            Payload=dumps({"csv_location": csv_location}).encode(),
        )
        payload = loads(response["Payload"].read().decode())
        return payload.get("xlsx_s3_path")
    except Exception:  # pylint: disable=broad-except
        logger.exception("XLSX Lambda invocation failed for %s", csv_location)
        return None
