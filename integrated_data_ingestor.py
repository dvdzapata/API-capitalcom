#!/usr/bin/env python3
"""Integrated data ingestion script for multiple macroeconomic APIs."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def load_env_file(path: Path) -> Dict[str, str]:
    """Load simple KEY=VALUE pairs from a .env style file."""
    env_values: Dict[str, str] = {}
    if not path.exists():
        return env_values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_values[key.strip()] = value.strip().strip('"').strip("'")
    return env_values


def configure_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.WARNING)
    root_logger.addHandler(file_handler)


def slugify(value: str) -> str:
    allowed = []
    for char in value.lower():
        if char.isalnum():
            allowed.append(char)
        elif char in {"-", "_"}:
            allowed.append(char)
        else:
            allowed.append("-")
    slug = "".join(allowed)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "item"


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text in {"."}:
        return None
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def parse_any_date(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("/", "-")
    if text.endswith("-00"):
        text = text[:-3] + "-01"
    formats = [
        "%Y-%m-%d",
        "%Y-%m",
        "%Y%m%d",
        "%Y%m",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    upper = text.upper()
    if "Q" in upper:
        cleaned = upper.replace(" ", "")
        if "-" in cleaned:
            year_part, quarter_part = cleaned.split("-", 1)
        else:
            year_part = cleaned[:4]
            quarter_part = cleaned[4:]
        try:
            quarter = int(quarter_part.replace("Q", ""))
            month = (quarter - 1) * 3 + 1
            return datetime(int(year_part), month, 1)
        except ValueError:
            return None
    if "M" in upper and len(text) >= 6:
        year_part = text[:4]
        month_part = text[-2:]
        try:
            return datetime(int(year_part), int(month_part), 1)
        except ValueError:
            return None
    return None


def normalize_date_string(value: Any) -> Optional[str]:
    parsed = parse_any_date(value)
    if parsed is None:
        return None
    return parsed.strftime("%Y-%m-%d")


def expand_compact_index(index: int, sizes: Sequence[int]) -> List[int]:
    coordinates: List[int] = []
    remainder = index
    for size in reversed(sizes):
        coordinates.append(remainder % size)
        remainder //= size
    coordinates.reverse()
    return coordinates


class RateLimiter:
    def __init__(self, min_interval: float = 1.0) -> None:
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._last_call = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delta = now - self._last_call
            if delta < self.min_interval:
                time.sleep(self.min_interval - delta)
            self._last_call = time.monotonic()


def create_retry_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


class CacheManager:
    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, api: str, identifier: str) -> Path:
        slug = f"{slugify(api)}_{slugify(identifier)}.json"
        return self.cache_dir / slug

    def load(self, api: str, identifier: str) -> Dict[str, Any]:
        path = self._cache_path(api, identifier)
        if not path.exists():
            return {}
        try:
            with path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except (json.JSONDecodeError, OSError):
            logging.warning("Cache corrupt for %s %s, recreating", api, identifier)
            return {}

    def save(self, api: str, identifier: str, payload: Dict[str, Any]) -> None:
        path = self._cache_path(api, identifier)
        tmp_path = path.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        tmp_path.replace(path)

    @staticmethod
    def _make_hashable(value: Any) -> Any:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return value

    @staticmethod
    def record_key(record: Dict[str, Any]) -> Tuple:
        items = []
        for key, value in sorted(record.items()):
            items.append((key, CacheManager._make_hashable(value)))
        return tuple(items)


class DatabaseWriter:
    def __init__(self, env: Dict[str, str], schema_dir: Path) -> None:
        self.dialect = env.get("DB_DIALECT", "sqlite").lower()
        self.schema_dir = schema_dir
        self.schema_dir.mkdir(parents=True, exist_ok=True)
        self.schema_cache: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()

        if self.dialect == "sqlite":
            db_path = env.get("DB_DATABASE", "data_ingestor.db")
            self.connection = sqlite3.connect(db_path, check_same_thread=False)
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.placeholder = "?"
        elif self.dialect in {"postgres", "postgresql"}:
            try:
                import psycopg2
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError("psycopg2 is required for PostgreSQL support") from exc
            self.connection = psycopg2.connect(
                host=env.get("DB_HOST"),
                port=env.get("DB_PORT", "5432"),
                user=env.get("DB_USER"),
                password=env.get("DB_PASSWORD"),
                dbname=env.get("DB_DATABASE"),
                connect_timeout=10,
            )
            self.placeholder = "%s"
        else:
            raise ValueError(f"Unsupported DB_DIALECT: {self.dialect}")

    def close(self) -> None:
        try:
            self.connection.close()
        except Exception:  # pragma: no cover
            logging.exception("Error closing database connection")

    def _schema_path(self, table: str) -> Path:
        return self.schema_dir / f"{slugify(table)}_schema.json"

    def _load_schema(self, table: str) -> Dict[str, Any]:
        if table in self.schema_cache:
            return self.schema_cache[table]
        path = self._schema_path(table)
        if path.exists():
            try:
                with path.open("r", encoding="utf-8") as handle:
                    schema = json.load(handle)
                    self.schema_cache[table] = schema
                    return schema
            except (json.JSONDecodeError, OSError):
                logging.warning("Could not read schema cache for %s, rebuilding", table)
        schema = {"columns": {}, "primary_key": []}
        self.schema_cache[table] = schema
        return schema

    def _save_schema(self, table: str, schema: Dict[str, Any]) -> None:
        path = self._schema_path(table)
        tmp_path = path.with_suffix(".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(schema, handle, ensure_ascii=False, indent=2)
        tmp_path.replace(path)

    @staticmethod
    def _sanitize_column_name(name: str) -> str:
        allowed = []
        for char in name:
            if char.isalnum() or char == "_":
                allowed.append(char)
            else:
                allowed.append("_")
        sanitized = "".join(allowed).strip("_") or "col"
        if sanitized[0].isdigit():
            sanitized = f"_{sanitized}"
        return sanitized.lower()

    def _determine_primary_key(self, column_map: Dict[str, str], records: Iterable[Dict[str, Any]]) -> List[str]:
        candidates: List[Tuple[int, int, str]] = []
        exclude_names = {"value", "footnotes", "latest", "realtime_start", "realtime_end"}
        for original, sanitized in column_map.items():
            lower = original.lower()
            if lower in exclude_names or lower.endswith("_name") or lower.endswith("_label"):
                continue
            values = [record.get(original) for record in records if original in record]
            non_null = bool(values) and all(value not in (None, "") for value in values)
            priority = 0 if lower in {"date", "time", "time_period"} else 1
            candidates.append((priority, 0 if non_null else 1, sanitized))
        if not candidates:
            return sorted(column_map.values())
        candidates.sort()
        return [item[2] for item in candidates]

    def _ensure_schema(self, table: str, records: Iterable[Dict[str, Any]]) -> Dict[str, str]:
        schema = self._load_schema(table)
        column_map: Dict[str, str] = schema.setdefault("columns", {})
        reverse_lookup = {v: k for k, v in column_map.items()}

        for record in records:
            for key in record.keys():
                if key in column_map:
                    continue
                base = self._sanitize_column_name(key)
                candidate = base
                counter = 1
                while candidate in reverse_lookup:
                    candidate = f"{base}_{counter}"
                    counter += 1
                column_map[key] = candidate
                reverse_lookup[candidate] = key

        if not schema.get("primary_key"):
            schema["primary_key"] = self._determine_primary_key(column_map, records)

        table_exists = self._table_exists(table)
        if not table_exists:
            self._create_table(table, column_map, schema["primary_key"])
        else:
            existing_columns = self._existing_columns(table)
            for sanitized in column_map.values():
                if sanitized not in existing_columns:
                    self._add_column(table, sanitized)
                    existing_columns.add(sanitized)

        self._save_schema(table, schema)
        return column_map

    def _table_exists(self, table: str) -> bool:
        cursor = self.connection.cursor()
        if self.dialect == "sqlite":
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
            )
            exists = cursor.fetchone() is not None
        else:
            cursor.execute(
                "SELECT to_regclass(%s)", (table,)
            )
            exists = cursor.fetchone()[0] is not None
        cursor.close()
        return exists

    def _existing_columns(self, table: str) -> set:
        cursor = self.connection.cursor()
        columns: set = set()
        try:
            if self.dialect == "sqlite":
                cursor.execute(f"PRAGMA table_info('{table}')")
                for row in cursor.fetchall():
                    columns.add(row[1])
            else:
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name=%s",
                    (table,),
                )
                for row in cursor.fetchall():
                    columns.add(row[0])
        except Exception:
            columns = set()
        finally:
            cursor.close()
        return columns

    def _create_table(self, table: str, column_map: Dict[str, str], primary_key: List[str]) -> None:
        columns_sql = []
        for original, sanitized in column_map.items():
            columns_sql.append(f'"{sanitized}" TEXT')
        pk_clause = ", ".join(f'"{col}"' for col in primary_key)
        sql = f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(columns_sql)}, PRIMARY KEY ({pk_clause}))'
        cursor = self.connection.cursor()
        cursor.execute(sql)
        self.connection.commit()
        cursor.close()

    def _add_column(self, table: str, column: str) -> None:
        cursor = self.connection.cursor()
        cursor.execute(f'ALTER TABLE "{table}" ADD COLUMN "{column}" TEXT')
        self.connection.commit()
        cursor.close()

    def store_records(self, table: str, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        with self.lock:
            column_map = self._ensure_schema(table, records)
            ordered_keys = list(column_map.keys())
            column_names = [column_map[key] for key in ordered_keys]
            placeholders = ", ".join([self.placeholder] * len(column_names))
            insert_sql: str
            if self.dialect == "sqlite":
                insert_sql = (
                    f'INSERT OR REPLACE INTO "{table}" ({", ".join(f'"{col}"' for col in column_names)}) '
                    f'VALUES ({placeholders})'
                )
            else:
                pk = self._load_schema(table)["primary_key"]
                update_clause = ", ".join(
                    f'"{col}" = EXCLUDED."{col}"' for col in column_names
                )
                insert_sql = (
                    f'INSERT INTO "{table}" ({", ".join(f'"{col}"' for col in column_names)}) '
                    f'VALUES ({placeholders}) '
                    f'ON CONFLICT ({", ".join(f'"{col}"' for col in pk)}) DO UPDATE SET {update_clause}'
                )

            cursor = self.connection.cursor()
            rows = []
            for record in records:
                row = []
                for key in ordered_keys:
                    value = record.get(key)
                    row.append(value if value is None else str(value))
                rows.append(tuple(row))
            try:
                cursor.executemany(insert_sql, rows)
                self.connection.commit()
            finally:
                cursor.close()


class DataProvider:
    def __init__(self, session: requests.Session, rate_limiter: RateLimiter) -> None:
        self.session = session
        self.rate_limiter = rate_limiter
        self.logger = logging.getLogger(self.__class__.__name__)

    def fetch(self, identifier: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @staticmethod
    def _safe_request(session: requests.Session, method: str, url: str, **kwargs: Any) -> requests.Response:
        response = session.request(method, url, timeout=kwargs.pop("timeout", 60), **kwargs)
        response.raise_for_status()
        return response


class FREDProvider(DataProvider):
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, api_key: Optional[str]) -> None:
        super().__init__(create_retry_session(), RateLimiter(0.6))
        self.api_key = api_key

    def fetch(self, identifier: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        params = {
            "series_id": identifier,
            "file_type": "json",
            "observation_start": start_date,
            "observation_end": end_date,
        }
        if self.api_key:
            params["api_key"] = self.api_key
        self.rate_limiter.wait()
        response = self._safe_request(self.session, "GET", self.BASE_URL, params=params)
        payload = response.json()
        if "observations" not in payload:
            raise ValueError(f"Unexpected response for FRED series {identifier}")
        records: List[Dict[str, Any]] = []
        for item in payload["observations"]:
            date = normalize_date_string(item.get("date"))
            record = {
                "date": date,
                "value": parse_float(item.get("value")),
                "realtime_start": item.get("realtime_start"),
                "realtime_end": item.get("realtime_end"),
                "series_id": identifier,
            }
            if item.get("footnotes"):
                record["footnotes"] = item.get("footnotes")
            records.append(record)
        return records


class CensusProvider(DataProvider):
    BASE_URL = "https://api.census.gov/data"

    def __init__(self, api_key: Optional[str]) -> None:
        super().__init__(create_retry_session(), RateLimiter(0.8))
        self.api_key = api_key

    @staticmethod
    def _split_identifier(identifier: str) -> Tuple[str, Dict[str, str]]:
        if "?" in identifier:
            path, query = identifier.split("?", 1)
        else:
            path, query = identifier, ""
        params: Dict[str, str] = {}
        for part in filter(None, query.split("&")):
            if "=" in part:
                key, value = part.split("=", 1)
                params[key] = value
        return path.strip("/"), params

    def fetch(self, identifier: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        path, params = self._split_identifier(identifier)
        if self.api_key:
            params.setdefault("key", self.api_key)
        start_year = start_date[:4]
        params.setdefault("time", f"from+{start_year}")
        url = f"{self.BASE_URL}/{path}"
        self.rate_limiter.wait()
        response = self._safe_request(self.session, "GET", url, params=params)
        payload = response.json()
        if not payload or not isinstance(payload, list) or len(payload) < 2:
            raise ValueError(f"Unexpected Census response for {identifier}")
        headers = payload[0]
        records: List[Dict[str, Any]] = []
        for row in payload[1:]:
            record = {headers[i]: row[i] for i in range(len(headers))}
            record["date"] = normalize_date_string(record.get("time"))
            record["value"] = parse_float(record.get("value") or record.get("cell_value"))
            records.append(record)
        return records


class BLSProvider(DataProvider):
    BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    def __init__(self, api_key: Optional[str]) -> None:
        super().__init__(create_retry_session(), RateLimiter(1.0))
        self.api_key = api_key

    def fetch(self, identifier: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        payload: Dict[str, Any] = {
            "seriesid": [identifier],
            "startyear": start_year,
            "endyear": end_year,
        }
        if self.api_key:
            payload["registrationkey"] = self.api_key
        self.rate_limiter.wait()
        response = self._safe_request(self.session, "POST", self.BASE_URL, json=payload)
        payload_json = response.json()
        if payload_json.get("status") != "REQUEST_SUCCEEDED":
            raise ValueError(f"BLS request failed for {identifier}: {payload_json.get('message')}")
        series_data = payload_json.get("Results", {}).get("series", [])
        if not series_data:
            return []
        data = series_data[0].get("data", [])
        records: List[Dict[str, Any]] = []
        for entry in data:
            period = entry.get("period", "")
            year = entry.get("year")
            date = self._bls_period_to_date(year, period)
            record = {
                "date": date,
                "value": parse_float(entry.get("value")),
                "period": period,
                "period_name": entry.get("periodName"),
                "latest": entry.get("latest"),
                "series_id": identifier,
            }
            if entry.get("footnotes"):
                record["footnotes"] = entry.get("footnotes")
            records.append(record)
        return records

    @staticmethod
    def _bls_period_to_date(year: Optional[str], period: str) -> Optional[str]:
        if not year:
            return None
        year_int = int(year)
        if period.startswith("M"):
            month = int(period[1:])
            return datetime(year_int, month, 1).strftime("%Y-%m-%d")
        if period.startswith("Q"):
            quarter = int(period[1:])
            month = (quarter - 1) * 3 + 1
            return datetime(year_int, month, 1).strftime("%Y-%m-%d")
        if period.startswith("S"):
            semester = int(period[1:])
            month = (semester - 1) * 6 + 1
            return datetime(year_int, month, 1).strftime("%Y-%m-%d")
        if period == "A01":
            return datetime(year_int, 1, 1).strftime("%Y-%m-%d")
        return None


class EurostatProvider(DataProvider):
    BASE_URL = "https://ec.europa.eu/eurostat/api/discover/data"

    def __init__(self) -> None:
        super().__init__(create_retry_session(), RateLimiter(1.0))

    @staticmethod
    def _split_identifier(identifier: str) -> Tuple[str, Dict[str, str]]:
        if "?" in identifier:
            dataset, query = identifier.split("?", 1)
        else:
            dataset, query = identifier, ""
        params: Dict[str, str] = {}
        for part in filter(None, query.split("&")):
            if "=" in part:
                key, value = part.split("=", 1)
                params[key] = value
        return dataset.strip("/"), params

    def fetch(self, identifier: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        dataset, params = self._split_identifier(identifier)
        params.setdefault("time", f">={start_date[:4]}")
        url = f"{self.BASE_URL}/{dataset}"
        self.rate_limiter.wait()
        response = self._safe_request(self.session, "GET", url, params=params)
        payload = response.json()
        value_map = payload.get("value", {})
        dimensions = payload.get("dimension", {})
        ids = payload.get("id", [])
        sizes = payload.get("size", [])
        if not value_map or not ids or not sizes:
            raise ValueError(f"Unexpected Eurostat response for {identifier}")

        dimension_values: Dict[str, List[Dict[str, Any]]] = {}
        for dim_id in ids:
            dim_info = dimensions.get(dim_id, {})
            values = dim_info.get("category", {}).get("index", {})
            labels = dim_info.get("category", {}).get("label", {})
            ordered: List[Dict[str, Any]] = [None] * len(values)
            for key, idx in values.items():
                ordered[idx] = {"id": key, "name": labels.get(key)}
            dimension_values[dim_id] = ordered

        records: List[Dict[str, Any]] = []
        for index_str, value in value_map.items():
            idx = int(index_str)
            coords = expand_compact_index(idx, sizes)
            record: Dict[str, Any] = {}
            for dim_position, dim_id in enumerate(ids):
                dim_options = dimension_values.get(dim_id, [])
                if dim_position < len(coords) and coords[dim_position] < len(dim_options):
                    dim_value = dim_options[coords[dim_position]]
                    record[dim_id.lower()] = dim_value.get("id")
                    if dim_value.get("name"):
                        record[f"{dim_id.lower()}_name"] = dim_value.get("name")
            record["value"] = parse_float(value)
            time_value = record.get("time") or record.get("time_period")
            record["date"] = normalize_date_string(time_value)
            records.append(record)
        return records


class ECBProvider(DataProvider):
    BASE_URL = "https://sdw-wsrest.ecb.europa.eu/service/data"

    def __init__(self) -> None:
        super().__init__(create_retry_session(), RateLimiter(1.0))

    def fetch(self, identifier: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        dataset, series = self._split_identifier(identifier)
        params = {
            "startPeriod": start_date,
            "endPeriod": end_date,
            "format": "sdmx-json",
        }
        url = f"{self.BASE_URL}/{dataset}/{series}"
        self.rate_limiter.wait()
        response = self._safe_request(self.session, "GET", url, params=params)
        payload = response.json()
        return self._parse_sdmx_json(payload)

    @staticmethod
    def _split_identifier(identifier: str) -> Tuple[str, str]:
        parts = identifier.strip("/").split("/", 1)
        if len(parts) != 2:
            raise ValueError("ECB identifier must be in the form DATASET/SERIES")
        return parts[0], parts[1]

    def _parse_sdmx_json(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        structure = payload.get("structure", {})
        data_sets = payload.get("dataSets", [])
        if not structure or not data_sets:
            return []
        series_dims = structure.get("dimensions", {}).get("series", [])
        obs_dims = structure.get("dimensions", {}).get("observation", [])
        attr_defs = structure.get("attributes", {}).get("observation", [])
        data_series = data_sets[0].get("series", {})

        series_lookup: Dict[str, List[Dict[str, Any]]] = {}
        for dim in series_dims:
            values = dim.get("values", [])
            series_lookup[dim.get("id")] = values

        obs_lookup: List[List[Dict[str, Any]]] = []
        obs_sizes: List[int] = []
        for dim in obs_dims:
            values = dim.get("values", [])
            obs_lookup.append(values)
            obs_sizes.append(len(values))

        attr_lookup: List[List[Dict[str, Any]]] = []
        for attr in attr_defs:
            values = attr.get("values", [])
            attr_lookup.append(values)

        records: List[Dict[str, Any]] = []
        for series_key, series_content in data_series.items():
            series_indices = [int(part) for part in series_key.split(":") if part != ""]
            series_metadata: Dict[str, Any] = {}
            for idx, dim in zip(series_indices, series_dims):
                dim_values = series_lookup.get(dim.get("id"), [])
                if idx < len(dim_values):
                    value_obj = dim_values[idx]
                    series_metadata[dim.get("id").lower()] = value_obj.get("id")
                    if value_obj.get("name"):
                        series_metadata[f"{dim.get('id').lower()}_name"] = value_obj.get("name")

            observations = series_content.get("observations", {})
            for obs_key, obs_values in observations.items():
                obs_index = int(obs_key) if obs_key.isdigit() else None
                if obs_index is None and ":" in obs_key:
                    parts = [int(p) for p in obs_key.split(":")]
                elif obs_index is not None and obs_sizes:
                    parts = expand_compact_index(obs_index, obs_sizes)
                else:
                    parts = [obs_index] if obs_index is not None else []

                record = dict(series_metadata)
                for position, dim in enumerate(obs_dims):
                    dim_values = obs_lookup[position]
                    coord = parts[position] if position < len(parts) else None
                    if coord is not None and 0 <= coord < len(dim_values):
                        value_obj = dim_values[coord]
                        record[dim.get("id").lower()] = value_obj.get("id")
                        if value_obj.get("name"):
                            record[f"{dim.get('id').lower()}_name"] = value_obj.get("name")
                value = obs_values[0] if obs_values else None
                record["value"] = parse_float(value)
                if len(obs_values) > 1 and attr_lookup:
                    for attr_index, attr_dim in enumerate(attr_defs):
                        attr_idx = obs_values[attr_index + 1]
                        attr_values = attr_lookup[attr_index]
                        if attr_idx is not None and attr_idx < len(attr_values):
                            attr_obj = attr_values[attr_idx]
                            record[attr_dim.get("id").lower()] = attr_obj.get("id")
                time_value = record.get("time_period") or record.get("time")
                record["date"] = normalize_date_string(time_value)
                records.append(record)
        return records


class IMFProvider(DataProvider):
    BASE_URL = "https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData"

    def __init__(self) -> None:
        super().__init__(create_retry_session(), RateLimiter(1.2))

    def fetch(self, identifier: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        dataset, series = self._split_identifier(identifier)
        params = {
            "startPeriod": start_date,
            "endPeriod": end_date,
        }
        url = f"{self.BASE_URL}/{dataset}/{series}"
        self.rate_limiter.wait()
        response = self._safe_request(self.session, "GET", url, params=params)
        payload = response.json()
        return self._parse_compact(payload)

    @staticmethod
    def _split_identifier(identifier: str) -> Tuple[str, str]:
        parts = identifier.strip("/").split("/", 1)
        if len(parts) != 2:
            raise ValueError("IMF identifier must be in the form DATASET/SERIES")
        return parts[0], parts[1]

    def _parse_compact(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        compact = payload.get("CompactData", {})
        dataset = compact.get("DataSet")
        if not dataset:
            return []
        series = dataset.get("Series")
        series_list: List[Dict[str, Any]]
        if isinstance(series, list):
            series_list = series
        elif isinstance(series, dict):
            series_list = [series]
        else:
            return []
        records: List[Dict[str, Any]] = []
        for series_entry in series_list:
            base_metadata = {
                key.lstrip("@" ).lower(): value
                for key, value in series_entry.items()
                if key.startswith("@") and key not in {"@FREQ"}
            }
            observations = series_entry.get("Obs", [])
            if isinstance(observations, dict):
                observations = [observations]
            for obs in observations:
                record = dict(base_metadata)
                time_value = obs.get("@TIME_PERIOD")
                record["date"] = normalize_date_string(time_value)
                record["value"] = parse_float(obs.get("@OBS_VALUE"))
                for key, value in obs.items():
                    if key.startswith("@") and key not in {"@TIME_PERIOD", "@OBS_VALUE"}:
                        record[key.lstrip("@").lower()] = value
                records.append(record)
        return records


class BEAProvider(DataProvider):
    BASE_URL = "https://apps.bea.gov/api/data/"

    def __init__(self, api_key: Optional[str]) -> None:
        super().__init__(create_retry_session(), RateLimiter(0.7))
        self.api_key = api_key

    @staticmethod
    def _parse_identifier(identifier: str) -> Dict[str, str]:
        params: Dict[str, str] = {}
        for part in filter(None, identifier.replace(";", "&").split("&")):
            if "=" in part:
                key, value = part.split("=", 1)
                params[key] = value
            else:
                params.setdefault("DataSetName", part)
        return params

    def fetch(self, identifier: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        params = self._parse_identifier(identifier)
        params.setdefault("method", "GetData")
        params.setdefault("ResultFormat", "JSON")
        if self.api_key:
            params.setdefault("UserID", self.api_key)
        start_year = int(start_date[:4])
        end_year = int(end_date[:4])
        params.setdefault(
            "Year",
            ",".join(str(year) for year in range(start_year, end_year + 1)),
        )
        self.rate_limiter.wait()
        response = self._safe_request(self.session, "GET", self.BASE_URL, params=params)
        payload = response.json()
        results = payload.get("BEAAPI", {}).get("Results", {})
        data = results.get("Data") or results.get("Series")
        if not data:
            return []
        if isinstance(data, dict):
            data = data.get("Data") or data.get("Series") or []
        records: List[Dict[str, Any]] = []
        for item in data:
            record = {key.lower(): value for key, value in item.items()}
            time_value = record.get("timeperiod") or record.get("time")
            record["date"] = normalize_date_string(time_value)
            record["value"] = parse_float(record.get("datavalue") or record.get("value"))
            records.append(record)
        return records


@dataclass
class SeriesConfig:
    api: str
    identifier: str
    table: str


def parse_config_file(path: Path) -> List[SeriesConfig]:
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    configs: List[SeriesConfig] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line or "," not in line:
            logging.warning("Skipping invalid config line: %s", raw_line)
            continue
        api_name, remainder = line.split("=", 1)
        identifier, table = remainder.rsplit(",", 1)
        configs.append(
            SeriesConfig(
                api=api_name.strip().upper(),
                identifier=identifier.strip(),
                table=table.strip(),
            )
        )
    return configs


class DataIngestor:
    def __init__(self, env: Dict[str, str], config_path: Path) -> None:
        self.env = env
        self.config_path = config_path
        self.start_date = env.get("START_DATE", "2020-01-01")
        self.end_date = env.get("END_DATE") or datetime.utcnow().strftime("%Y-%m-%d")
        cache_dir = Path(env.get("CACHE_DIR", "data_cache"))
        self.cache_manager = CacheManager(cache_dir)
        schema_dir = cache_dir / "schemas"
        self.db_writer = DatabaseWriter(env, schema_dir)
        self.configs = parse_config_file(config_path)
        self.providers = self._initialize_providers()

    def _initialize_providers(self) -> Dict[str, DataProvider]:
        providers: Dict[str, DataProvider] = {}
        if any(cfg.api == "FRED" for cfg in self.configs):
            providers["FRED"] = FREDProvider(self.env.get("FRED_API_KEY"))
        if any(cfg.api == "CENSUS" for cfg in self.configs):
            providers["CENSUS"] = CensusProvider(self.env.get("CENSUS_API_KEY"))
        if any(cfg.api == "BLS" for cfg in self.configs):
            providers["BLS"] = BLSProvider(self.env.get("BLS_API_KEY"))
        if any(cfg.api == "EUROSTAT" for cfg in self.configs):
            providers["EUROSTAT"] = EurostatProvider()
        if any(cfg.api == "ECB" for cfg in self.configs):
            providers["ECB"] = ECBProvider()
        if any(cfg.api == "IMF" for cfg in self.configs):
            providers["IMF"] = IMFProvider()
        if any(cfg.api == "BEA" for cfg in self.configs):
            providers["BEA"] = BEAProvider(self.env.get("BEA_API_KEY"))
        return providers

    def run_once(self) -> None:
        for config in self.configs:
            provider = self.providers.get(config.api)
            if not provider:
                logging.error("No provider available for API %s", config.api)
                continue
            logging.info("Procesando %s -> %s", config.identifier, config.table)
            try:
                self._process_series(provider, config)
            except Exception as exc:  # pragma: no cover
                logging.exception("Error processing %s (%s): %s", config.identifier, config.api, exc)

    def _process_series(self, provider: DataProvider, config: SeriesConfig) -> None:
        cache_entry = self.cache_manager.load(config.api, config.identifier)
        existing_records = cache_entry.get("records", []) if cache_entry else []
        existing_keys = {self.cache_manager.record_key(rec) for rec in existing_records}
        last_cached_date = cache_entry.get("last_date") if cache_entry else None
        if last_cached_date:
            start_date = max(self.start_date, last_cached_date)
        else:
            start_date = self.start_date
        records = provider.fetch(config.identifier, start_date, self.end_date)
        new_records: List[Dict[str, Any]] = []
        for record in records:
            key = self.cache_manager.record_key(record)
            if key not in existing_keys:
                existing_keys.add(key)
                existing_records.append(record)
                new_records.append(record)
        if new_records:
            self.db_writer.store_records(config.table, new_records)
            existing_records.sort(key=lambda record: record.get("date") or "")
            last_date = self._compute_last_date(existing_records)
            cache_payload = {
                "api": config.api,
                "identifier": config.identifier,
                "table": config.table,
                "last_date": last_date,
                "records": existing_records,
                "updated_at": datetime.utcnow().isoformat(),
            }
            self.cache_manager.save(config.api, config.identifier, cache_payload)
            logging.info("%s: %d registros nuevos", config.identifier, len(new_records))
        else:
            logging.info("%s: sin novedades", config.identifier)

    @staticmethod
    def _compute_last_date(records: List[Dict[str, Any]]) -> Optional[str]:
        dates: List[datetime] = []
        for record in records:
            parsed = parse_any_date(record.get("date"))
            if parsed:
                dates.append(parsed)
        if not dates:
            return None
        return max(dates).strftime("%Y-%m-%d")


def main() -> None:
    env_path = Path(os.environ.get("ENV_FILE", ".env"))
    env_values = load_env_file(env_path)
    for key, value in env_values.items():
        os.environ.setdefault(key, value)
    log_file = Path(os.environ.get("LOG_FILE", "logs/data_ingestor.log"))
    configure_logging(log_file)
    config_path = Path(os.environ.get("INDEX_CONFIG_FILE", "indices_config.txt"))
    env = dict(os.environ)
    ingestor = DataIngestor(env, config_path)
    loop_mode = env.get("INGEST_LOOP", "false").lower() in {"true", "1", "yes"}
    interval_hours = float(env.get("INGEST_INTERVAL_HOURS", "24"))
    try:
        if not loop_mode:
            ingestor.run_once()
        else:
            logging.info("Modo continuo habilitado (intervalo %.2f horas)", interval_hours)
            while True:
                start_time = time.time()
                ingestor.run_once()
                elapsed = time.time() - start_time
                sleep_seconds = max(0.0, interval_hours * 3600 - elapsed)
                if sleep_seconds > 0:
                    logging.info("Durmiendo %.0f segundos", sleep_seconds)
                    time.sleep(sleep_seconds)
    except KeyboardInterrupt:
        logging.info("Interrupción manual recibida. Saliendo...")
    finally:
        ingestor.db_writer.close()


if __name__ == "__main__":
    main()
