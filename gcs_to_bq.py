import os
import sys
from datetime import datetime
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_RAW = os.getenv("BQ_DATASET_RAW", "raw_steam")
GCP_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")

if not GCP_PROJECT_ID:
    raise ValueError("Missing GCP_PROJECT_ID in .env")
if not GCP_BUCKET_NAME:
    raise ValueError("Missing GCP_BUCKET_NAME in .env")

def parse_selected_date() -> str:
    if len(sys.argv) == 1:
        return datetime.now().strftime("%Y-%m-%d")
    if len(sys.argv) == 2:
        try:
            datetime.strptime(sys.argv[1], "%Y-%m-%d")
            return sys.argv[1]
        except ValueError:
            print("Invalid date format. Use YYYY-MM-DD.")
            sys.exit(1)

    print("Usage: python load_raw_to_bigquery.py [YYYY-MM-DD] or no arguments for today's date")
    sys.exit(1)

def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    dataset_ref = f"{GCP_PROJECT_ID}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "EU"
    client.create_dataset(dataset, exists_ok=True)


def ensure_catalog_table(client: bigquery.Client) -> None:
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.steam_catalog"
    schema = [
        bigquery.SchemaField("partition_date", "DATE"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("appid", "INT64"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("last_modified", "INT64"),
        bigquery.SchemaField("price_change_number", "INT64"),
        bigquery.SchemaField("raw", "JSON"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="partition_date",
    )
    table.clustering_fields = ["appid"]
    client.create_table(table, exists_ok=True)


def ensure_appdetails_table(client: bigquery.Client) -> None:
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.steam_appdetails"
    schema = [
        bigquery.SchemaField("ingestion_date", "DATE"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("appid", "INT64"),
        bigquery.SchemaField("success", "BOOL"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("raw", "JSON"),
        bigquery.SchemaField("error_payload", "JSON"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingestion_date",
    )
    table.clustering_fields = ["appid"]
    client.create_table(table, exists_ok=True)

def ensure_reviews_table(client: bigquery.Client) -> None:
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.steam_reviews"
    schema = [
        bigquery.SchemaField("ingestion_date", "DATE"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("appid", "INT64"),
        bigquery.SchemaField("success", "BOOL"),
        bigquery.SchemaField("total_reviews", "INT64"),
        bigquery.SchemaField("good_reviews", "INT64"),
        bigquery.SchemaField("bad_reviews", "INT64"),
        bigquery.SchemaField("review_score", "INT64"),
        bigquery.SchemaField("review_score_desc", "STRING"),
        bigquery.SchemaField("num_reviews_returned", "INT64"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("raw", "JSON"),
        bigquery.SchemaField("error_payload", "JSON"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingestion_date",
    )
    table.clustering_fields = ["appid"]
    client.create_table(table, exists_ok=True)



def load_partition(
    client: bigquery.Client,
    table_name: str,
    gcs_uri: str,
    selected_date: str,
) -> None:
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.{table_name}"
    partition_column_name = "partition_date" if table_name == "steam_catalog" else "ingestion_date"
    delete_sql = f"""
    DELETE FROM `{table_id}`
    WHERE {partition_column_name} = DATE('{selected_date}')
    """
    client.query(delete_sql).result()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )
    load_job.result()

    print(f"Loaded {gcs_uri} into {table_id}")


def main():
    selected_date = parse_selected_date()
    client = bigquery.Client(project=GCP_PROJECT_ID)

    ensure_dataset(client, BQ_DATASET_RAW)
    ensure_catalog_table(client)
    ensure_appdetails_table(client)
    ensure_reviews_table(client)

    catalog_uri = (
        f"gs://{GCP_BUCKET_NAME}/data/raw/steam_catalog/steam_catalog_{selected_date}.jsonl"
    )
    appdetails_uri = (
        f"gs://{GCP_BUCKET_NAME}/data/raw/steam_appdetails/steam_appdetails_{selected_date}.jsonl"
    )
    reviews_uri = (
        f"gs://{GCP_BUCKET_NAME}/data/raw/steam_reviews/steam_reviews_{selected_date}.jsonl"
    )

    load_partition(client, "steam_catalog", catalog_uri, selected_date)
    load_partition(client, "steam_appdetails", appdetails_uri, selected_date)
    load_partition(client, "steam_reviews", reviews_uri, selected_date)

    print("Done.")


if __name__ == "__main__":
    main()