import io
import os
import argparse
import boto3
import polars as pl
from dotenv import load_dotenv

def jsonl_to_parquet(source_key, dest_key):
    load_dotenv()

    s3 = boto3.client("s3")

    source_bucket = os.getenv("S3_BUCKET_NAME")

    # 1. Read JSON bytes from S3 using boto3
    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    json_buffer = io.BytesIO(response["Body"].read())

    # 2. Parse JSON with Polars
    # Use pl.read_json() for standard JSON arrays: [{"id": 1}, {"id": 2}]
    # Use pl.read_ndjson() for newline-delimited JSON: {"id": 1}\n{"id": 2}
    df = pl.read_ndjson(json_buffer)

    # 3. Write DataFrame to an in-memory Parquet buffer
    parquet_buffer = io.BytesIO()
    df.write_parquet(parquet_buffer, compression="zstd")

    # 4. Upload Parquet backup to S3 using boto3
    parquet_buffer.seek(0)
    s3.upload_fileobj(parquet_buffer, source_bucket, dest_key)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Run JSONL to parquet after pdf metadata extraction."
    )
    parser.add_argument(
        "--source_key",
        type=str,
        default=None,
        help="source of JSONL files",
    )
    parser.add_argument(
        "--dest_key",
        type=str,
        default=None,
        help="destination of parquet",
    )

    args = parser.parse_args()
    jsonl_to_parquet(args.source_key, args.dest_key)


    