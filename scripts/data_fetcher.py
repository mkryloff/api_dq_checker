import os
import json
import random

import requests
from datetime import datetime, timezone

API_URL = "https://v6.exchangerate-api.com/v6/c05a71229182f756475273b3/latest/USD"
RAW_FILE_PATH = "./result"


def fetch_and_corrupt_data():
    response = requests.get(API_URL)

    if response.status_code != 200:
        raise Exception(f"Ошибка REST API: получен статус-код {response.status_code}")

    data = response.json()

    report_date = datetime.now(timezone.utc).isoformat()
    rates = data.get("conversion_rates", {})

    records = []
    for currency, rate in rates.items():
        records.append({
            "report_date": report_date,
            "currency_code": currency,
            "rate": float(rate)
        })

    if len(records) > 5:

        first_record = records[random.randint(0, len(records) - 1)].copy()
        records.append(first_record)

        records[random.randint(0, len(records) - 1)]["rate"] = None

        if records[random.randint(0, len(records) - 1)]["rate"] is not None:
            records[random.randint(0, len(records) - 1)]["rate"] = records[random.randint(0, len(records) - 1)]["rate"] * 5.0

    os.makedirs(os.path.dirname(RAW_FILE_PATH), exist_ok=True)
    with open(RAW_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=4, ensure_ascii=False)
