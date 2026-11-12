import os
import argparse
from io import BytesIO
import boto3
import polars as pl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

# Configuration
BASE_URL = "https://api.openalex.org/works"
SOURCE_BUCKET = os.getenv("S3_BUCKET_NAME")
DEST_BUCKET = os.getenv("S3_BUCKET_NAME")
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY")  # Optional: add to your .env
MAILTO = os.getenv("POLITE_EMAIL", "your-email@example.com")

KEEP_COLS = ["document_id", "uuid", "pdf_name", "s3_key"]

# Explicit Polars schema ensures columns that are all-null in a batch don't fail or infer as Null type
OPENALEX_SCHEMA = {
    "openalex_id": pl.Utf8,
    "doi": pl.Utf8,
    "mag_id": pl.Utf8,
    "pmid": pl.Utf8,
    "openalex_title": pl.Utf8,
    "publication_year": pl.Int64,
    "publication_date": pl.Utf8,
    "type": pl.Utf8,
    "language": pl.Utf8,
    "cited_by_count": pl.Int64,
    "is_oa": pl.Boolean,
    "oa_status": pl.Utf8,
    "oa_url": pl.Utf8,
    "source_name": pl.Utf8,
    "issn_l": pl.Utf8,
    "authors": pl.List(pl.Utf8),
    "primary_topic": pl.Utf8,
    "relevance_score": pl.Float64,
}

# S3 client
s3 = boto3.client("s3")

# Requests session with automatic retries for 429 / 5xx errors
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
session.mount("https://", HTTPAdapter(max_retries=retries))


def build_params(title: str) -> dict:
    # OpenAlex separates multiple filter clauses with commas, so replace commas in the title
    clean_title = title.strip().replace(",", " ")
    params = {
        "filter": f"title.search:{clean_title}",
        "per_page": 1,
        "mailto": MAILTO,
    }
    if OPENALEX_API_KEY:
        params["api_key"] = OPENALEX_API_KEY
    return params


def fetch_work_by_title(title: str) -> dict | None:
    response = session.get(
        BASE_URL,
        params=build_params(title),
        timeout=30,
    )
    response.raise_for_status()

    results = response.json().get("results", [])
    return results[0] if results else None


def extract_openalex_metadata(work: dict | None) -> dict:
    if not work:
        return {
            "openalex_id": None,
            "doi": None,
            "mag_id": None,
            "pmid": None,
            "openalex_title": None,
            "publication_year": None,
            "publication_date": None,
            "type": None,
            "language": None,
            "cited_by_count": None,
            "is_oa": None,
            "oa_status": None,
            "oa_url": None,
            "source_name": None,
            "issn_l": None,
            "authors": [],
            "primary_topic": None,
            "relevance_score": None,
        }

    ids_dict = work.get("ids") or {}
    primary_source = (work.get("primary_location") or {}).get("source") or {}
    open_access = work.get("open_access") or {}

    authors = [
        a.get("author", {}).get("display_name")
        for a in (work.get("authorships") or [])
        if a.get("author", {}).get("display_name")
    ]

    return {
        "openalex_id": work.get("id"),
        "doi": work.get("doi"),
        "mag_id": ids_dict.get("mag"),
        "pmid": ids_dict.get("pmid"),
        "openalex_title": work.get("title"),
        "publication_year": work.get("publication_year"),
        "publication_date": work.get("publication_date"),
        "type": work.get("type"),
        "language": work.get("language"),
        "cited_by_count": work.get("cited_by_count"),
        "is_oa": open_access.get("is_oa"),
        "oa_status": open_access.get("oa_status"),
        "oa_url": open_access.get("oa_url"),
        "source_name": primary_source.get("display_name"),
        "issn_l": primary_source.get("issn_l"),
        "authors": authors,
        "primary_topic": (work.get("primary_topic") or {}).get("display_name"),
        "relevance_score": work.get("relevance_score"),
    }


def process_s3_parquet(
    source_bucket: str,
    source_key: str,
    dest_bucket: str,
    dest_key: str,
) -> pl.DataFrame:
    # 1. Read source Parquet from S3
    response = s3.get_object(
        Bucket=source_bucket,
        Key=source_key,
    )
    df = pl.read_parquet(BytesIO(response["Body"].read()))
    print(f"Loaded {len(df)} documents")

    # 2. Keep only the 4 target columns + title, and skip null/empty titles
    valid_df = (
        df.select(KEEP_COLS + ["title"])
        .filter(
            pl.col("title").is_not_null()
            & (pl.col("title").str.strip_chars() != "")
        )
    )
    print(f"Documents with valid titles: {len(valid_df)}/{len(df)}")

    # 3. Fetch OpenAlex metadata
    metadata_rows = []
    for i, title in enumerate(valid_df["title"].to_list()):
        try:
            work = fetch_work_by_title(title)
            metadata = extract_openalex_metadata(work)
        except Exception as e:
            print(f"Error processing row {i}, title={title!r}: {e}")
            metadata = extract_openalex_metadata(None)

        metadata_rows.append(metadata)

        if (i + 1) % 100 == 0:
            print(f"Processed {i + 1}/{len(valid_df)} documents")

    # 4. Convert metadata to Polars DataFrame with explicit schema
    metadata_df = pl.DataFrame(metadata_rows, schema=OPENALEX_SCHEMA)

    # 5. Combine only the 4 kept columns with the OpenAlex columns
    result_df = valid_df.select(KEEP_COLS).hstack(metadata_df)

    print(f"Output shape: {result_df.shape}")
    print(result_df.head())

    # 6. Write Parquet to S3
    buffer = BytesIO()
    result_df.write_parquet(buffer)
    buffer.seek(0)

    s3.upload_fileobj(
        buffer,
        dest_bucket,
        dest_key,
    )
    print(f"Saved output to s3://{dest_bucket}/{dest_key}")

    return result_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-key",
        required=True,
        help="S3 key of input Parquet file",
    )
    parser.add_argument(
        "--dest-key",
        required=True,
        help="S3 key of output Parquet file",
    )
    args = parser.parse_args()

    process_s3_parquet(
        source_bucket=SOURCE_BUCKET,
        source_key=args.source_key,
        dest_bucket=DEST_BUCKET,
        dest_key=args.dest_key,
    )