
import requests
import boto3
import polars as pl

from io import BytesIO


# Configuration
BASE_URL = "https://api.crossref.org/works"

SOURCE_BUCKET = "your-bucket"
SOURCE_KEY = "path/to/input.parquet"

DEST_BUCKET = "your-bucket"
DEST_KEY = "path/to/output.parquet"

MAILTO = "your-email@example.com"


# S3 client
s3 = boto3.client("s3")

# Crossref session
session = requests.Session()


def params(title):
    return {
        "query.title": title,
        "rows": 1,
        "mailto": MAILTO,
    }


def fetch_bib_by_title(title):
    response = session.get(
        BASE_URL,
        params=params(title),
        timeout=30,
    )
    response.raise_for_status()

    items = response.json()["message"]["items"]
    return items[0] if items else None


def extract_crossref_metadata(work):
    if not work:
        return {
            "crossref_title": None,
            "subtitle": None,
            "authors": [],
            "editors": [],
            "translators": [],
            "publication_year": None,
            "publisher": None,
            "publication_place": None,
            "edition": None,
            "document_type": None,
            "venue": None,
            "volume": None,
            "issue_number": None,
            "ISBN": None,
            "DOI": None,
            "ISSN": None,
        }

    # Publication year
    date = (
        work.get("published-print")
        or work.get("published-online")
        or work.get("published")
        or work.get("issued")
    )

    publication_year = (
        date["date-parts"][0][0]
        if date and date.get("date-parts")
        else None
    )

    # Contributors
    def get_names(role):
        return [
            (
                person.get("name")
                or " ".join(filter(None, [
                    person.get("given"),
                    person.get("family"),
                ]))
            )
            for person in work.get(role, [])
        ]

    return {
        "crossref_title": (work.get("title") or [None])[0],
        "subtitle": (work.get("subtitle") or [None])[0],
        "authors": get_names("author"),
        "editors": get_names("editor"),
        "translators": get_names("translator"),
        "publication_year": publication_year,
        "publisher": work.get("publisher"),
        "publication_place": work.get("publisher-location"),
        "edition": work.get("edition-number"),
        "document_type": work.get("type"),
        "venue": (work.get("container-title") or [None])[0],
        "volume": work.get("volume"),
        "issue_number": work.get("issue"),
        "ISBN": work.get("ISBN"),
        "DOI": work.get("DOI"),
        "ISSN": work.get("ISSN"),
    }


def process_s3_parquet(
    source_bucket,
    source_key,
    dest_bucket,
    dest_key,
):

    # 1. Read source Parquet from S3
    response = s3.get_object(
        Bucket=source_bucket,
        Key=source_key,
    )

    df = pl.read_parquet(
        BytesIO(response["Body"].read())
    )

    print(f"Loaded {len(df)} documents")

    # 2. Skip null, empty, and whitespace-only titles
    valid_df = df.filter(
        pl.col("title").is_not_null()
        & (pl.col("title").str.strip_chars() != "")
    )

    print(
        f"Documents with valid titles: "
        f"{len(valid_df)}/{len(df)}"
    )

    # 3. Fetch Crossref metadata
    metadata_rows = []

    for i, title in enumerate(valid_df["title"].to_list()):

        try:
            work = fetch_bib_by_title(title)
            metadata = extract_crossref_metadata(work)

        except Exception as e:
            print(
                f"Error processing row {i}, "
                f"title={title!r}: {e}"
            )

            metadata = extract_crossref_metadata(None)

        metadata_rows.append(metadata)

        if (i + 1) % 100 == 0:
            print(
                f"Processed {i + 1}/"
                f"{len(valid_df)} documents"
            )

    # 4. Convert metadata to Polars DataFrame
    metadata_df = pl.DataFrame(metadata_rows)

    # 5. Append metadata to original columns
    result_df = valid_df.hstack(metadata_df)

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

    print(
        f"Saved output to "
        f"s3://{dest_bucket}/{dest_key}"
    )

    return result_df


if __name__ == "__main__":

    result = process_s3_parquet(
        source_bucket=SOURCE_BUCKET,
        source_key=SOURCE_KEY,
        dest_bucket=DEST_BUCKET,
        dest_key=DEST_KEY,
    )
