# API Source Ingestion Framework

A config-driven, modular framework built on **Python** and **PySpark** in **Databricks** to ingest nested JSON data from REST API sources and write it to **Azure Data Lake Storage Gen2 (ADLS Gen2)**.

---

## Overview

This framework ingests transactional data (bills, items, customer details, pricing) from REST API sources into a data lakehouse landing layer. It is designed to be reusable across multiple API sources and outlets by driving all ingestion parameters through a per-outlet JSON config file.

The source system used as a reference implementation is **Posist**, a restaurant management platform, ingesting bill-level data across multiple food court outlets.

---

## Architecture

```
REST API Source (Posist)
        │
        │  Bearer Token Auth
        │  GET /api/v1/pos  (paginated, watermark-based)
        ▼
┌─────────────────────────────────┐
│     posist_bills_ingestion      │  ← Main ingestion notebook
│  • Config-driven per outlet     │
│  • Watermark for incremental    │
│  • Pagination support           │
│  • History load support         │
└─────────────────────────────────┘
        │
        │  write_api_response() → JSON files
        ▼
ADLS Gen2 - Landing Container
  └── landing/posist/bills/{outlet}/{date}/{page}/
        │
        ▼  [downstream - Silver layer]
┌─────────────────────────────────┐
│    flatten_nested_json()        │  ← Flattens nested structs + explodes arrays
└─────────────────────────────────┘
        │
        ▼
ADLS Gen2 - Silver Layer (Delta tables)
```

---

## Project Structure

```
api-source-ingestion/
│
├── src/
│   └── purchase_orders/
│       ├── utility_api.ipynb                # APIClient class + ADLS writer
│       └── utility_timestamps.ipynb         # Unix timestamp + IST conversion utilities
│       └── posist_bills_ingestion.ipynb     # Main ingestion notebook (Posist source)
│       └── flatten_nested_json.ipynb        # Reusable nested JSON flattening utility
│
├── config/
│   └── posist/
│       └── posist_{outlet}_config.json      # Per-outlet config file (one per outlet)
│
└── README.md
```

---

## Notebooks

### 1. `utility_api.ipynb` - API Client & ADLS Writer

Defines the `APIClient` class for interacting with REST APIs, supporting multiple authentication methods.

**Supported Auth Types:**

| Auth Type | Config Key |
|---|---|
| Bearer Token | `bearer_token` |
| API Key | `api_key` |
| Basic Auth | `basic` |

**Key functions:**

```python
# Initialise with base URL, headers, and auth config
client = APIClient(
    base_url="https://api.example.com",
    auth={"type": "bearer_token", "access_token": "<token>"}
)

# Make a GET request to an endpoint with optional params
response = client.get(endpoint="bills", params={"from": 123456, "to": 789012})

# Write the API JSON response to ADLS Gen2 as a JSON file
write_api_response(response, target_path="abfss://landing@<account>.dfs.core.windows.net/posist/bills/outlet1/2026-05-26/page_1")
```

---

### 2. `utility_timestamps.ipynb` - Timestamp Utilities

Provides helper functions for Unix timestamp generation and IST (Indian Standard Time) conversion.

**Key functions:**

```python
# Get list of (start, end) Unix timestamp pairs for a date range
# Supports: both dates, start only, or neither (defaults to previous day)
timestamps = get_unix_timestamps(start_date="2026-01-01", end_date="2026-01-31")
# Returns: [(start_ts_1, end_ts_1), (start_ts_2, end_ts_2), ...]

# Convert a Unix timestamp (ms) to an IST date string
date_str = convert_unix_timestamp_to_date(unix_timestamp=1714521600000)
# Returns: "2026-05-01 00:00:00"

# Get current date in IST (midnight, formatted as YYYY-MM-DD HH:MM:SS)
current_date = get_current_date()
# Returns: "2026-05-26 00:00:00"
```

**IST offset:** UTC + 5:30 hours (19800000 ms)

**Supported date formats:**
- `YYYY-MM-DD HH:MM:SS`
- `YYYY-MM-DD`

---

### 3. `posist_bills_ingestion.ipynb` - Main Ingestion Notebook

Ingests bill data from the Posist API to the ADLS Gen2 landing layer. Driven entirely by a per-outlet JSON config file.

**Ingestion modes:**

| Mode | Trigger | Description |
|---|---|---|
| Incremental | `history_load.enabled = false` | Fetches data between `watermark_old` and `watermark_new` |
| History Load | `history_load.enabled = true` | Fetches data between `history_load.start_date` and `history_load.end_date` |

**Helper functions:**

| Function | Description |
|---|---|
| `get_bills()` | Fetches paginated bill data from Posist API for a given time range using `APIClient` |
| `row_to_dict()` | Recursively converts a PySpark Row object to a Python dictionary for JSON serialization |
| `update_config_file()` | Writes updated `watermark_old` (or `history_load.start_date`) back to the config JSON on ADLS after each run |

**Execution flow:**
1. Reads widget parameters (`storage_account`, `container`, `outlet`, `config_path`)
2. Loads outlet-specific config from ADLS
3. Determines timestamp ranges (incremental or history)
4. Iterates over each timestamp range, fetching paginated API responses
5. Writes each page as a JSON file to the landing layer
6. Updates `watermark_old` in the config file after successful ingestion

**Output path pattern:**
```
{target_path}/{process_date}/posist_bills_{outlet}_{bills_date}_{page}.json
```
Example:
```
landing/posist/bills/<outlet>/pending/2026-05-26/<outlet>_20260526_1.json
```

---

### 4. `flatten_nested_json.ipynb` - Nested JSON Flattening Utility

Reusable PySpark utility for flattening deeply nested JSON structures. Used in downstream Silver layer processing.

**Key functions:**

```python
# Flatten all nested StructType columns (joins field names with ".")
df_flat = flatten_structs(nested_df)

# Flatten AND explode ArrayType columns (with configurable exclusions)
df_flat = flatten_df(df, arrays_to_not_explode=["products"])
```

**Example - Input schema:**
```
root
 └── data: struct
      ├── customer_name: string
      ├── order_details: struct
      │    ├── total_amount: double
      │    └── items: array
      │         ├── item_name: string
      │         ├── quantity: int
      │         └── unit_price: double
      └── outlet: string
```

**After `flatten_df()`:**
```
root
 ├── data.customer_name: string
 ├── data.order_details.total_amount: double
 ├── data.order_details.items.item_name: string     ← exploded from array
 ├── data.order_details.items.quantity: int
 ├── data.order_details.items.unit_price: double
 └── data.outlet: string
```

---

## Config File Structure

Each outlet has its own config JSON stored in ADLS. Below is the structure:

```json
{
  "access_token": "<bearer-token>",
  "active": true,
  "customer_key": "<customer-key>",
  "history_load": {
    "enabled": false,
    "start_date": "2023-01-01 00:00:00",
    "end_date": "2023-01-01 00:00:00"
  },
  "is_highway": 1,
  "outlet": "<outlet-name>",
  "target_path": "landing/posist/bills/<outlet-name>/pending",
  "watermark_new": "2026-05-26 00:00:00",
  "watermark_old": "2026-05-26 00:00:00"
}
```

**Config fields:**

| Field | Description |
|---|---|
| `access_token` | Bearer token for Posist API authentication |
| `active` | Whether this outlet is active for ingestion |
| `customer_key` | Outlet-specific customer key for API filtering |
| `history_load.enabled` | Set `true` to trigger a history load |
| `history_load.start_date` | Start date for history load |
| `history_load.end_date` | End date for history load |
| `is_highway` | Flag to identify highway outlets |
| `outlet` | Outlet name (used in file path) |
| `target_path` | ADLS landing path for output files |
| `watermark_new` | Upper bound for incremental load |
| `watermark_old` | Lower bound for incremental load (updated after each run) |

---

## Watermark Mechanism

The framework uses a **dual-watermark** approach for incremental ingestion:

```
Run 1:  watermark_old = "2026-05-01"  →  watermark_new = "2026-05-02"
        [fetch data, write to landing]
        [update config: watermark_old = "2026-05-02"]

Run 2:  watermark_old = "2026-05-02"  →  watermark_new = "2026-05-03"
        ...
```

After each successful run, `watermark_old` is updated in the config file on ADLS, ensuring no data is re-fetched or missed on the next run.

---

## Prerequisites

- Databricks (PySpark environment)
- Azure Data Lake Storage Gen2
- `dbutils` (available natively in Databricks)
- Python packages: `requests`, `logging`, `datetime`

---

## Getting Started

1. Clone this repository.
2. Upload the config JSON for each outlet to your ADLS config container:
   ```
   landing/posist/config/posist_{outlet}_config.json
   ```
3. In Databricks, set the notebook widgets:
   ```
   storage_account  = "<your-storage-account>"
   container        = "landing"
   outlet           = "<outlet-name>"
   config_path      = "landing/posist/config/posist_{outlet}_config.json"
   ```
4. Import `utility_api` and `utility_timestamps` as shared utilities from your Databricks workspace:
   ```python
   %run /Workspace/Shared/posist/utilities/utility_api
   %run /Workspace/Shared/posist/utilities/utility_timestamps
   ```
5. Run `posist_bills_ingestion.ipynb`.

---

## Extending to a New API Source

To onboard a new API source:

1. Create a new config JSON for each outlet/entity following the same structure.
2. Reuse `utility_api.ipynb` - just change the `base_url` and `auth` type.
3. Reuse `utility_timestamps.ipynb` as-is for any timestamp-based pagination.
4. Create a new ingestion notebook following the same pattern as `posist_bills_ingestion.ipynb`.
5. Use `flatten_nested_json.ipynb` downstream in Silver processing for any nested response structure.

---

## Tech Stack

![Python](https://img.shields.io/badge/Python-3.x-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange)
![Databricks](https://img.shields.io/badge/Databricks-enabled-red)
![Azure ADLS](https://img.shields.io/badge/Azure-ADLS%20Gen2-blue)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-enabled-green)
![REST API](https://img.shields.io/badge/REST-API%20Ingestion-lightgrey)
