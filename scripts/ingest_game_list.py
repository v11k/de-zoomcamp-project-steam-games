import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
import requests
from dotenv import load_dotenv

load_dotenv()

STEAM_API_KEY = os.getenv("STEAM_API_KEY")
BASE_DIR = Path(__file__).resolve().parent.parent
BASE_URL = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
MAX_RESULTS = 50_000
REQUEST_TIMEOUT = 60
SLEEP_SECONDS = 1

OUTPUT_DIR = BASE_DIR / "data" / "raw" / "steam_catalog"

def get_page(last_appid: int = 0) -> dict:
    if not STEAM_API_KEY:
        raise ValueError("Missing STEAM_API_KEY in .env")

    payload = {
        "include_games": True,
        "include_dlc": False,
        "include_software": False,
        "include_videos": False,
        "include_hardware": False,
        "max_results": MAX_RESULTS,
        "last_appid": last_appid,
    }

    params = {
        "key": STEAM_API_KEY,
        "input_json": json.dumps(payload),
    }

    response = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()

def fetch_all_games() -> list[dict]:
    all_apps = []
    last_appid = 0
    page_num = 1

    while True:
        print(f"Fetching page {page_num} (last_appid={last_appid})...")
        data = get_page(last_appid=last_appid)

        response_block = data.get("response", {})
        apps = response_block.get("apps", [])

        if not apps:
            print("No apps returned. Stopping.")
            break

        all_apps.extend(apps)
        print(f"Fetched {len(apps)} apps. Total so far: {len(all_apps)}")

        last_appid = apps[-1]["appid"]
        have_more_results = response_block.get("have_more_results")

        if have_more_results is False:
            print("Steam says there are no more results.")
            break

        page_num += 1
        time.sleep(SLEEP_SECONDS)

    return all_apps


def write_jsonl(apps: list[dict]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    output_path = OUTPUT_DIR / f"steam_catalog_{run_date}.jsonl"

    with output_path.open("w", encoding="utf-8") as f:
        for app in apps:
            row = {
                "ingested_at_utc": run_date,
                "partition_date": run_date,
                "source": "steam_istoreservice_getapplist_v1",
                "appid": app.get("appid"),
                "name": app.get("name"),
                "last_modified": app.get("last_modified"),
                "price_change_number": app.get("price_change_number"),
                "raw": app,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    return output_path


def main():
    apps = fetch_all_games()
    output_path = write_jsonl(apps)

    print(f"\nDone.")
    print(f"Total apps ingested: {len(apps)}")
    print(f"Output file: {output_path}")


if __name__ == "__main__":
    main()