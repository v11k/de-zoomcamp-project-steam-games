import argparse
import json
import random
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from requests import Response, Session

CATALOG_DIR = Path("data/raw/steam_catalog")
OUTPUT_DIR = Path("data/raw/steam_appdetails")
FAILED_DIR = Path("data/raw/steam_appdetails_failed")

APPDETAILS_URL = "https://store.steampowered.com/api/appdetails"
COUNTRY_CODE = "us"
LANGUAGE = "english"

REQUEST_TIMEOUT = 30
CHECKPOINT_EVERY = 200

# Observed-safe limiter
RATE_LIMIT_REQUESTS = 180
RATE_LIMIT_WINDOW_SECONDS = 300

MAX_RETRIES = 8
BACKOFF_BASE_SECONDS = 2
BACKOFF_MAX_SECONDS = 120


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=valid_date,
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="Partition date in YYYY-MM-DD format. Defaults to today's UTC date.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit for testing.",
    )
    return parser.parse_args()


def valid_date(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError as e:
        raise argparse.ArgumentTypeError("Date must be in YYYY-MM-DD format") from e


def get_catalog_file_for_date(selected_date: str) -> Path:
    path = CATALOG_DIR / f"steam_catalog_{selected_date}.jsonl"
    if path.exists():
        return path
    raise FileNotFoundError(f"Catalog file not found: {path}")


def get_latest_catalog_file() -> Path:
    files = sorted(CATALOG_DIR.glob("steam_catalog_*.jsonl"))
    if not files:
        raise FileNotFoundError(f"No catalog JSONL files found in {CATALOG_DIR}")
    return files[-1]


def read_appids_from_catalog(catalog_file: Path) -> list[int]:
    appids = []

    with catalog_file.open("r", encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            appid = row.get("appid")
            if appid is not None:
                appids.append(int(appid))

    # preserve order, remove duplicates
    return list(dict.fromkeys(appids))


def get_processed_appids(output_path: Path) -> set[int]:
    processed = set()

    if not output_path.exists():
        return processed

    with output_path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
                appid = row.get("appid")
                if appid is not None:
                    processed.add(int(appid))
            except json.JSONDecodeError:
                continue

    return processed


def trim_appdetails(data: dict) -> dict:
    return {
        "steam_appid": data.get("steam_appid"),
        "type": data.get("type"),
        "name": data.get("name"),
        "is_free": data.get("is_free"),
        "required_age": data.get("required_age"),
        "developers": data.get("developers"),
        "publishers": data.get("publishers"),
        "price_overview": data.get("price_overview"),
        "platforms": data.get("platforms"),
        "categories": data.get("categories"),
        "genres": data.get("genres"),
        "recommendations": data.get("recommendations"),
        "release_date": data.get("release_date"),
        "metacritic": data.get("metacritic"),
        "content_descriptors": data.get("content_descriptors"),
    }


def extract_row(appid: int, payload: dict, ingested_at: str, partition_date: str) -> dict:
    app_key = str(appid)
    app_block = payload.get(app_key, {})
    success = app_block.get("success", False)
    data = app_block.get("data", {}) if success else {}

    trimmed = trim_appdetails(data) if success else None

    return {
        "ingested_at": ingested_at,
        "ingestion_date": partition_date,
        "source": "steam_store_appdetails",
        "appid": appid,
        "success": success,
        "type": trimmed.get("type") if trimmed else None,
        "name": trimmed.get("name") if trimmed else None,
        "raw": trimmed,
        "error_payload": None if success else app_block,
    }


def make_failure_row(appid: int, ingested_at: str, partition_date: str, error_text: str) -> dict:
    return {
        "ingested_at": ingested_at,
        "ingestion_date": partition_date,
        "source": "steam_store_appdetails",
        "appid": appid,
        "success": False,
        "type": None,
        "name": None,
        "raw": None,
        "error_payload": {"error": error_text},
    }


def calculate_backoff(attempt: int) -> float:
    exp = min(BACKOFF_BASE_SECONDS * (2 ** attempt), BACKOFF_MAX_SECONDS)
    jitter = random.uniform(0, 1)
    return exp + jitter


class RateLimiter:
    def __init__(self, max_requests: int, window_seconds: int) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_timestamps: deque[float] = deque()

    def wait(self) -> None:
        now = time.monotonic()

        while self.request_timestamps and now - self.request_timestamps[0] >= self.window_seconds:
            self.request_timestamps.popleft()

        if len(self.request_timestamps) >= self.max_requests:
            sleep_for = self.window_seconds - (now - self.request_timestamps[0]) + 1
            print(f"Rate limit window full. Sleeping for {sleep_for:.1f}s")
            time.sleep(sleep_for)

            now = time.monotonic()
            while self.request_timestamps and now - self.request_timestamps[0] >= self.window_seconds:
                self.request_timestamps.popleft()

        self.request_timestamps.append(time.monotonic())


def get_retry_after_seconds(response: Response) -> Optional[float]:
    retry_after = response.headers.get("Retry-After")
    if not retry_after:
        return None

    try:
        return min(float(retry_after), BACKOFF_MAX_SECONDS)
    except ValueError:
        return None


def fetch_appdetails(session: Session, rate_limiter: RateLimiter, appid: int) -> dict:
    params = {
        "appids": appid,
        "cc": COUNTRY_CODE,
        "l": LANGUAGE,
    }

    for attempt in range(MAX_RETRIES):
        response = None
        try:
            rate_limiter.wait()
            response = session.get(APPDETAILS_URL, params=params, timeout=REQUEST_TIMEOUT)

            if response.status_code == 429:
                retry_after_seconds = get_retry_after_seconds(response)
                wait_time = retry_after_seconds if retry_after_seconds is not None else calculate_backoff(attempt)
                print(f"429 for appid={appid}. Retry {attempt + 1}/{MAX_RETRIES} in {wait_time:.1f}s")
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise

            wait_time = calculate_backoff(attempt)
            status = response.status_code if response is not None else "no_response"
            print(
                f"Request failed for appid={appid} "
                f"(status={status}). Retry {attempt + 1}/{MAX_RETRIES} in {wait_time:.1f}s. Error: {e}"
            )
            time.sleep(wait_time)

    raise RuntimeError(f"Failed to fetch appdetails for appid={appid} after {MAX_RETRIES} retries")


def write_rows(rows: list[dict], output_path: Path) -> None:
    with output_path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_failed_appids(failed_appids: list[int], failed_path: Path) -> None:
    if not failed_appids:
        return

    with failed_path.open("a", encoding="utf-8") as f:
        for appid in failed_appids:
            f.write(f"{appid}\n")


def main() -> None:
    args = parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    FAILED_DIR.mkdir(parents=True, exist_ok=True)

    try:
        catalog_file = get_catalog_file_for_date(args.date)
    except FileNotFoundError:
        catalog_file = get_latest_catalog_file()
        print(f"No catalog file for {args.date}. Falling back to latest: {catalog_file}")

    appids = read_appids_from_catalog(catalog_file)

    partition_date = args.date
    ingested_at = datetime.now(timezone.utc).isoformat()

    output_path = OUTPUT_DIR / f"steam_appdetails_{partition_date}.jsonl"
    failed_path = FAILED_DIR / f"steam_appdetails_failed_{partition_date}.txt"

    processed_appids = get_processed_appids(output_path)
    if processed_appids:
        print(f"Found {len(processed_appids)} already processed appids in {output_path}")

    appids = [appid for appid in appids if appid not in processed_appids]

    if args.limit is not None:
        appids = appids[:args.limit]

    total = len(appids)
    if total == 0:
        print("No appids left to process.")
        return

    print(f"Starting/resuming appdetails ingestion for {total} appids")
    print(f"Rate limiter: {RATE_LIMIT_REQUESTS} requests / {RATE_LIMIT_WINDOW_SECONDS} seconds")

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "steam-games-pipeline/1.0",
            "Accept": "application/json",
        }
    )

    rate_limiter = RateLimiter(
        max_requests=RATE_LIMIT_REQUESTS,
        window_seconds=RATE_LIMIT_WINDOW_SECONDS,
    )

    buffer: list[dict] = []
    failed_appids: list[int] = []

    for idx, appid in enumerate(appids, start=1):
        try:
            payload = fetch_appdetails(
                session=session,
                rate_limiter=rate_limiter,
                appid=appid,
            )
            row = extract_row(
                appid=appid,
                payload=payload,
                ingested_at=ingested_at,
                partition_date=partition_date,
            )
            buffer.append(row)

        except requests.RequestException as e:
            print(f"Permanent failure for appid={appid}: {e}")
            buffer.append(
                make_failure_row(
                    appid=appid,
                    ingested_at=ingested_at,
                    partition_date=partition_date,
                    error_text=str(e),
                )
            )
            failed_appids.append(appid)

        except Exception as e:
            print(f"Unexpected failure for appid={appid}: {e}")
            buffer.append(
                make_failure_row(
                    appid=appid,
                    ingested_at=ingested_at,
                    partition_date=partition_date,
                    error_text=str(e),
                )
            )
            failed_appids.append(appid)

        if len(buffer) >= CHECKPOINT_EVERY:
            write_rows(buffer, output_path)
            write_failed_appids(failed_appids, failed_path)
            print(f"Wrote {idx}/{total} rows in this run")
            buffer = []
            failed_appids = []

    if buffer:
        write_rows(buffer, output_path)

    if failed_appids:
        write_failed_appids(failed_appids, failed_path)

    print(f"Done. Output file: {output_path}")
    print(f"Failed appids file: {failed_path}")


if __name__ == "__main__":
    main()