#!/usr/bin/env python3
"""Auditoría y completitud de series temporales financieras.

El script conecta con PostgreSQL, detecta elementos huérfanos, analiza las
series configuradas y rellena huecos derivados de forma local o consultando
Capital.com, Financial Modeling Prep o Yahoo Finance. Registra todas las
acciones en archivos JSON versionados y muestra un resumen en consola.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import math
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

try:  # Dependencia opcional
    import yfinance as yf  # type: ignore

    HAS_YFINANCE = True
except ModuleNotFoundError:  # pragma: no cover - rama opcional
    HAS_YFINANCE = False

PLACEHOLDER_VALUES = {
    "",
    " ",
    "na",
    "nan",
    "null",
    "none",
    "-",
    "sin_dato",
    "placeholder",
    "pending",
}

NUMERIC_FIELDS = {
    "open",
    "high",
    "low",
    "close",
    "volume",
    "divadj_open",
    "divadj_high",
    "divadj_low",
    "divadj_close",
    "change",
    "change_percent",
    "open_bid",
    "open_ask",
    "close_bid",
    "close_ask",
    "spread_open",
    "spread_close",
}

INTEGER_FIELDS = {"ts_epoch"}

CAPITAL_DEFAULT_FIELD_MAP = {
    "open_bid": "openPrice.bid",
    "open_ask": "openPrice.ask",
    "close_bid": "closePrice.bid",
    "close_ask": "closePrice.ask",
    "open": "openPrice.mid",
    "high": "highPrice.mid",
    "low": "lowPrice.mid",
    "close": "closePrice.mid",
    "volume": "lastTradedVolume",
    "spread_open": "openPrice.spread",
    "spread_close": "closePrice.spread",
}

FMP_DAILY_FIELD_MAP = {
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "divadj_close": "adjClose",
    "divadj_open": "adjOpen",
    "divadj_high": "adjHigh",
    "divadj_low": "adjLow",
    "change": "change",
    "change_percent": "changePercent",
}

FMP_INTRADAY_FIELD_MAP = {
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
}

YFINANCE_FIELD_MAP = {
    "open": "Open",
    "high": "High",
    "low": "Low",
    "close": "Close",
    "volume": "Volume",
}

DEFAULT_TABLES = [
    {
        "table": "cotizaciones_diarias_cfd",
        "asset_column": "asset_id",
        "datetime_column": "fecha",
        "provider": "capital",
        "frequency": "daily",
        "timezone": "Europe/Madrid",
        "field_map": CAPITAL_DEFAULT_FIELD_MAP,
    },
    {
        "table": "cotizaciones_intradia_cfd",
        "asset_column": "asset_id",
        "datetime_column": "timestamp",
        "provider": "capital",
        "frequency": "intraday",
        "interval_minutes": 5,
        "timezone": "Europe/Madrid",
        "field_map": CAPITAL_DEFAULT_FIELD_MAP,
    },
    {
        "table": "cotizaciones_diarias",
        "asset_column": "asset_id",
        "datetime_column": "fecha",
        "provider": "fmp",
        "frequency": "daily",
        "timezone": "UTC",
        "field_map": FMP_DAILY_FIELD_MAP,
    },
    {
        "table": "cotizaciones_intradia",
        "asset_column": "asset_id",
        "datetime_column": "timestamp",
        "provider": "fmp",
        "frequency": "intraday",
        "interval_minutes": 5,
        "timezone": "UTC",
        "field_map": FMP_INTRADAY_FIELD_MAP,
    },
]


@dataclass
class TableConfig:
    table: str
    asset_column: str
    datetime_column: str
    provider: str
    frequency: str
    fields: List[str] = field(default_factory=list)
    timezone: str = "UTC"
    interval_minutes: Optional[int] = None
    skip_weekends: bool = True
    symbol_column: Optional[str] = None
    endpoint: Optional[str] = None
    request_limit: int = 250
    field_map: Dict[str, str] = field(default_factory=dict)

    @staticmethod
    def from_dict(raw: Dict[str, Any]) -> "TableConfig":
        return TableConfig(
            table=raw["table"],
            asset_column=raw.get("asset_column", "asset_id"),
            datetime_column=raw.get("datetime_column", "fecha"),
            provider=raw.get("provider", "capital"),
            frequency=raw.get("frequency", "daily"),
            fields=list(raw.get("fields", [])),
            timezone=raw.get("timezone", "UTC"),
            interval_minutes=raw.get("interval_minutes"),
            skip_weekends=raw.get("skip_weekends", True),
            symbol_column=raw.get("symbol_column"),
            endpoint=raw.get("endpoint"),
            request_limit=int(raw.get("request_limit", 250)),
            field_map=raw.get("field_map", {}),
        )


@dataclass
class AppConfig:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    schema: str
    asset_table: str
    asset_id_column: str
    asset_symbol_column: str
    output_dir: Path
    logs_dir: Path
    timeseries_tables: List[TableConfig]
    capital_api_key: Optional[str]
    capital_api_url: Optional[str]
    fmp_api_key: Optional[str]
    fmp_api_url: str
    yfinance_enabled: bool
    max_refill_attempts: int

    @staticmethod
    def from_env(env_path: Optional[Path] = None) -> "AppConfig":
        if env_path is None:
            env_path = Path(".env")
        if env_path.exists():
            load_dotenv(env_path)

        tables_raw: List[Dict[str, Any]]
        tables_str = os.getenv("TIMESERIES_TABLES_JSON", "").strip()
        if tables_str:
            try:
                tables_raw = json.loads(tables_str)
            except json.JSONDecodeError as exc:
                logging.getLogger("completitud").warning(
                    "No se pudo parsear TIMESERIES_TABLES_JSON: %s", exc
                )
                tables_raw = DEFAULT_TABLES
        else:
            tables_raw = DEFAULT_TABLES

        timeseries_tables = [TableConfig.from_dict(t) for t in tables_raw]

        output_dir = Path(os.getenv("OUTPUT_DIR", "salidas"))
        logs_dir = Path(os.getenv("LOGS_DIR", "logs"))

        return AppConfig(
            db_host=os.getenv("PGHOST", "localhost"),
            db_port=int(os.getenv("PGPORT", "5432")),
            db_name=os.getenv("PGDATABASE", "capital"),
            db_user=os.getenv("PGUSER", "postgres"),
            db_password=os.getenv("PGPASSWORD", "postgres"),
            schema=os.getenv("PGSCHEMA", "public"),
            asset_table=os.getenv("ASSET_TABLE", "activos"),
            asset_id_column=os.getenv("ASSET_ID_COLUMN", "id"),
            asset_symbol_column=os.getenv("ASSET_SYMBOL_COLUMN", "simbolo"),
            output_dir=output_dir,
            logs_dir=logs_dir,
            timeseries_tables=timeseries_tables,
            capital_api_key=os.getenv("CAPITAL_API_KEY"),
            capital_api_url=os.getenv("CAPITAL_API_URL"),
            fmp_api_key=os.getenv("FMP_API_KEY"),
            fmp_api_url=os.getenv("FMP_API_URL", "https://financialmodelingprep.com/api/v3"),
            yfinance_enabled=os.getenv("ENABLE_YFINANCE", "1") not in {"0", "false", "False"},
            max_refill_attempts=int(os.getenv("MAX_REFILL_ATTEMPTS", "3")),
        )


def setup_logging(cfg: AppConfig) -> None:
    cfg.logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = cfg.logs_dir / f"completitud_{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


class Database:
    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.log = logging.getLogger("completitud.db")
        try:
            self.conn = psycopg2.connect(
                host=cfg.db_host,
                port=cfg.db_port,
                dbname=cfg.db_name,
                user=cfg.db_user,
                password=cfg.db_password,
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
        except psycopg2.OperationalError as exc:
            self.log.error(
                "No se pudo conectar a PostgreSQL %s@%s:%s/%s: %s",
                cfg.db_user,
                cfg.db_host,
                cfg.db_port,
                cfg.db_name,
                exc,
            )
            raise
        self.conn.autocommit = False

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.close()

    def fetchall(self, query: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            rows = list(cur.fetchall())
        return rows

    def fetchone(self, query: str, params: Optional[Sequence[Any]] = None) -> Optional[Dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
        return row

    def execute(self, query: str, params: Optional[Sequence[Any]] = None) -> None:
        with self.conn.cursor() as cur:
            cur.execute(query, params)

    def executemany(self, query: str, params_seq: Sequence[Sequence[Any]]) -> None:
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, query, params_seq, page_size=500)

    def list_columns(self, table: str) -> List[str]:
        sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        rows = self.fetchall(sql, (self.cfg.schema, table))
        return [r["column_name"] for r in rows]


class SchemaInspector:
    def __init__(self, db: Database, cfg: AppConfig):
        self.db = db
        self.cfg = cfg
        self.log = logging.getLogger("completitud.schema")

    def list_objects(self) -> Dict[str, List[str]]:
        sql = """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
        """
        rows = self.db.fetchall(sql, (self.cfg.schema,))
        objects: Dict[str, List[str]] = {"BASE TABLE": [], "VIEW": []}
        for row in rows:
            objects.setdefault(row["table_type"], []).append(row["table_name"])
        return objects

    def find_orphans(self) -> List[str]:
        findings: List[str] = []
        asset_table = self.cfg.asset_table
        asset_col = self.cfg.asset_id_column

        asset_ids = {
            row[asset_col]
            for row in self.db.fetchall(
                f"SELECT {asset_col} FROM {self.cfg.schema}.{asset_table}"
            )
        }

        for table_cfg in self.cfg.timeseries_tables:
            table = f"{self.cfg.schema}.{table_cfg.table}"
            column_list = self.db.list_columns(table_cfg.table)
            if table_cfg.asset_column not in column_list:
                findings.append(
                    f"La tabla {table_cfg.table} no tiene la columna {table_cfg.asset_column}"
                )
                continue

            nulls = self.db.fetchone(
                f"SELECT COUNT(*) AS cnt FROM {table} WHERE {table_cfg.asset_column} IS NULL"
            )
            if nulls and nulls["cnt"]:
                findings.append(
                    f"{table_cfg.table}: {nulls['cnt']} filas con asset_id nulo"
                )

            if asset_ids:
                missing = self.db.fetchall(
                    f"SELECT DISTINCT {table_cfg.asset_column} AS asset_id "
                    f"FROM {table} WHERE {table_cfg.asset_column} IS NOT NULL "
                    f"AND {table_cfg.asset_column} NOT IN %s",
                    (tuple(asset_ids),),
                )
                if missing:
                    ids = ", ".join(str(r["asset_id"]) for r in missing[:10])
                    findings.append(
                        f"{table_cfg.table}: asset_id sin correspondencia ({ids})"
                    )
        return findings

    def report(self) -> str:
        objects = self.list_objects()
        orphan_findings = self.find_orphans()
        lines = ["Objetos del esquema:"]
        lines.append(
            "  Tablas: " + ", ".join(sorted(objects.get("BASE TABLE", [])))
        )
        lines.append("  Vistas: " + ", ".join(sorted(objects.get("VIEW", []))))
        if orphan_findings:
            lines.append("Elementos huérfanos o inconsistentes:")
            lines.extend(f"  - {item}" for item in orphan_findings)
        else:
            lines.append("No se detectaron elementos huérfanos.")
        report = "\n".join(lines)
        self.log.info("%s", report.replace("\n", " | "))
        return report


class AssetResolver:
    def __init__(self, db: Database, cfg: AppConfig):
        self.db = db
        self.cfg = cfg
        self.log = logging.getLogger("completitud.assets")
        self.cache: Dict[Any, Dict[str, Any]] = {}

    def load(self) -> None:
        try:
            sql = (
                f"SELECT {self.cfg.asset_id_column} AS asset_id, {self.cfg.asset_symbol_column} AS symbol "
                f"FROM {self.cfg.schema}.{self.cfg.asset_table}"
            )
            rows = self.db.fetchall(sql)
        except Exception as exc:
            self.log.error("No se pudieron cargar los activos: %s", exc)
            rows = []
        self.cache = {row["asset_id"]: row for row in rows if row.get("asset_id") is not None}
        self.log.info("Cargados %d activos", len(self.cache))

    def get(self, asset_id: Any) -> Optional[Dict[str, Any]]:
        if not self.cache:
            self.load()
        return self.cache.get(asset_id)


class GapAnalysisResult:
    def __init__(self) -> None:
        self.tables: Dict[str, Dict[Any, Dict[str, Any]]] = defaultdict(dict)

    def add_asset(self, table: TableConfig, asset_id: Any, payload: Dict[str, Any]) -> None:
        self.tables[table.table][asset_id] = payload

    def to_json(self) -> Dict[str, Any]:
        return self.tables

    def iter_missing(self) -> Iterable[Tuple[TableConfig, Any, Dict[str, Any]]]:
        for table_name, assets in self.tables.items():
            for asset_id, payload in assets.items():
                if payload.get("missing_dates") or payload.get("field_gaps"):
                    yield table_name, asset_id, payload


def _resolve_tz(tz_name: str) -> dt.tzinfo:
    if tz_name.upper() == "UTC":
        return dt.timezone.utc
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return dt.datetime.now().astimezone().tzinfo or dt.timezone.utc


def to_timezone(value: dt.datetime, tz_name: str) -> dt.datetime:
    tzinfo = _resolve_tz(tz_name)
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(tzinfo)


def normalize_datetime(value: Any, tz_name: str) -> Optional[dt.datetime]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return to_timezone(value, tz_name)
    if isinstance(value, dt.date):
        tzinfo = _resolve_tz(tz_name)
        return dt.datetime.combine(value, dt.time(0, 0), tzinfo=tzinfo)
    raise TypeError(f"No puedo convertir {value!r} a datetime")


def is_placeholder(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in PLACEHOLDER_VALUES
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def extract_nested(record: Dict[str, Any], path: str) -> Any:
    current: Any = record
    for part in path.split("."):
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def map_remote_record(record: Dict[str, Any], field_map: Dict[str, str]) -> Dict[str, Any]:
    if not field_map:
        return dict(record)
    mapped: Dict[str, Any] = {}
    for local_field, remote_field in field_map.items():
        value = extract_nested(record, remote_field)
        if value is None and remote_field.endswith(".mid"):
            base = remote_field.rsplit(".", 1)[0]
            bid = extract_nested(record, f"{base}.bid")
            ask = extract_nested(record, f"{base}.ask")
            if bid is not None and ask is not None:
                try:
                    value = (float(bid) + float(ask)) / 2
                except (TypeError, ValueError):
                    value = None
        mapped[local_field] = value
    return mapped


class TimeseriesAnalyzer:
    def __init__(
        self,
        db: Database,
        cfg: AppConfig,
        assets: AssetResolver,
        max_assets: Optional[int] = None,
    ):
        self.db = db
        self.cfg = cfg
        self.assets = assets
        self.log = logging.getLogger("completitud.analyzer")
        self.max_assets = max_assets

    def run(self) -> GapAnalysisResult:
        result = GapAnalysisResult()
        for table_cfg in self.cfg.timeseries_tables:
            self.log.info("Analizando tabla %s", table_cfg.table)
            try:
                columns = self.db.list_columns(table_cfg.table)
            except Exception as exc:
                self.log.error("No se pudieron listar columnas de %s: %s", table_cfg.table, exc)
                continue

            if not table_cfg.fields:
                table_cfg.fields = [
                    c
                    for c in columns
                    if c not in {table_cfg.asset_column, table_cfg.datetime_column}
                ]

            asset_ids = self._list_asset_ids(table_cfg)
            for asset_id in asset_ids:
                payload = self._analyze_asset(table_cfg, asset_id)
                result.add_asset(table_cfg, asset_id, payload)
        return result

    def _list_asset_ids(self, table_cfg: TableConfig) -> List[Any]:
        sql = (
            f"SELECT DISTINCT {table_cfg.asset_column} AS asset_id "
            f"FROM {self.cfg.schema}.{table_cfg.table} "
            f"WHERE {table_cfg.asset_column} IS NOT NULL"
        )
        rows = self.db.fetchall(sql)
        asset_ids = [row["asset_id"] for row in rows]
        if self.max_assets is not None:
            return asset_ids[: self.max_assets]
        return asset_ids

    def _fetch_asset_frame(self, table_cfg: TableConfig, asset_id: Any) -> pd.DataFrame:
        columns = [table_cfg.datetime_column, table_cfg.asset_column] + table_cfg.fields
        sql = (
            f"SELECT {', '.join(columns)} "
            f"FROM {self.cfg.schema}.{table_cfg.table} "
            f"WHERE {table_cfg.asset_column} = %s "
            f"ORDER BY {table_cfg.datetime_column}"
        )
        rows = self.db.fetchall(sql, (asset_id,))
        frame = pd.DataFrame(rows)
        if frame.empty:
            return frame
        frame[table_cfg.datetime_column] = frame[table_cfg.datetime_column].apply(
            lambda v: normalize_datetime(v, table_cfg.timezone)
        )
        frame.set_index(table_cfg.datetime_column, inplace=True)
        return frame

    def _analyze_asset(self, table_cfg: TableConfig, asset_id: Any) -> Dict[str, Any]:
        frame = self._fetch_asset_frame(table_cfg, asset_id)
        if frame.empty:
            return {
                "asset_id": asset_id,
                "rows": 0,
                "missing_dates": [],
                "field_gaps": {},
                "semantic_issues": [],
                "derived_updates": 0,
            }

        derived_updates = self._fill_derived_fields(table_cfg, asset_id, frame)
        missing_dates = self._detect_missing_dates(table_cfg, frame)
        field_gaps = self._detect_field_gaps(frame)
        semantic_issues = self._semantic_checks(frame)

        return {
            "asset_id": asset_id,
            "rows": int(len(frame)),
            "missing_dates": [dt.isoformat() for dt in missing_dates],
            "field_gaps": field_gaps,
            "semantic_issues": semantic_issues,
            "derived_updates": derived_updates,
        }

    def _detect_missing_dates(
        self, table_cfg: TableConfig, frame: pd.DataFrame
    ) -> List[dt.datetime]:
        if frame.empty:
            return []
        index: pd.DatetimeIndex = frame.index.sort_values()
        start, end = index.min(), index.max()
        if table_cfg.frequency == "daily":
            freq = "B" if table_cfg.skip_weekends else "D"
        elif table_cfg.frequency == "intraday":
            interval = table_cfg.interval_minutes or 5
            minutes = f"{interval}min"
            freq = minutes
        else:
            freq = "D"
        expected = pd.date_range(start=start, end=end, freq=freq, tz=start.tz)
        missing = expected.difference(index)
        if table_cfg.frequency == "daily" and table_cfg.skip_weekends:
            missing = pd.DatetimeIndex(
                [d for d in missing if d.weekday() < 5]
            )
        return list(missing.to_pydatetime())

    def _detect_field_gaps(self, frame: pd.DataFrame) -> Dict[str, int]:
        gaps: Dict[str, int] = {}
        for column in frame.columns:
            series = frame[column]
            gap_mask = series.isna()
            if series.dtype == object:
                gap_mask |= series.map(is_placeholder)
            if column in {"volume"}:
                gap_mask |= series.fillna(0) == 0
            count = int(gap_mask.sum())
            if count:
                gaps[column] = count
        return gaps

    def _semantic_checks(self, frame: pd.DataFrame) -> List[str]:
        issues: List[str] = []
        for ts, row in frame.iterrows():
            open_p = row.get("open")
            high = row.get("high")
            low = row.get("low")
            close = row.get("close")
            volume = row.get("volume")
            if open_p is not None and high is not None and open_p > high:
                issues.append(f"{ts.isoformat()} open>high")
            if low is not None and close is not None and low > close:
                issues.append(f"{ts.isoformat()} low>close")
            if volume is not None and volume == 0:
                issues.append(f"{ts.isoformat()} volumen cero")
        return issues

    def _fill_derived_fields(
        self, table_cfg: TableConfig, asset_id: Any, frame: pd.DataFrame
    ) -> int:
        updates: List[Tuple[Any, dt.datetime, Dict[str, Any]]] = []
        for ts, row in frame.iterrows():
            update: Dict[str, Any] = {}
            # Dividend adjusted
            factor = None
            if not pd.isna(row.get("divadj_close")) and not pd.isna(row.get("close")):
                close = row.get("close")
                if close not in (None, 0):
                    factor = row.get("divadj_close") / close
            for field in ["open", "high", "low"]:
                target = f"divadj_{field}"
                if target in frame.columns and pd.isna(row.get(target)) and factor is not None:
                    base = row.get(field)
                    if base not in (None, 0):
                        update[target] = round(base * factor, 8)

            if pd.isna(row.get("change")) and not pd.isna(row.get("open")) and not pd.isna(row.get("close")):
                update["change"] = row.get("close") - row.get("open")
            if (
                pd.isna(row.get("change_percent"))
                and not pd.isna(row.get("open"))
                and row.get("open") not in (None, 0)
                and not pd.isna(row.get("close"))
            ):
                update["change_percent"] = (row.get("close") - row.get("open")) / row.get("open") * 100

            if "ts_epoch" in frame.columns and (pd.isna(row.get("ts_epoch")) or row.get("ts_epoch") == 0):
                update["ts_epoch"] = int(ts.timestamp())

            if (
                "spread_open" in frame.columns
                and pd.isna(row.get("spread_open"))
                and not pd.isna(row.get("open_bid"))
                and not pd.isna(row.get("open_ask"))
            ):
                update["spread_open"] = row.get("open_ask") - row.get("open_bid")
            if (
                "spread_close" in frame.columns
                and pd.isna(row.get("spread_close"))
                and not pd.isna(row.get("close_bid"))
                and not pd.isna(row.get("close_ask"))
            ):
                update["spread_close"] = row.get("close_ask") - row.get("close_bid")

            if (
                "open_bid" in frame.columns
                and pd.isna(row.get("open_bid"))
                and not pd.isna(row.get("open"))
                and not pd.isna(row.get("spread_open"))
            ):
                update["open_bid"] = row.get("open") - row.get("spread_open") / 2
            if (
                "open_ask" in frame.columns
                and pd.isna(row.get("open_ask"))
                and not pd.isna(row.get("open"))
                and not pd.isna(row.get("spread_open"))
            ):
                update["open_ask"] = row.get("open") + row.get("spread_open") / 2
            if (
                "open" in frame.columns
                and pd.isna(row.get("open"))
                and not pd.isna(row.get("open_bid"))
                and not pd.isna(row.get("open_ask"))
            ):
                update["open"] = (row.get("open_bid") + row.get("open_ask")) / 2

            if (
                "close_bid" in frame.columns
                and pd.isna(row.get("close_bid"))
                and not pd.isna(row.get("close"))
                and not pd.isna(row.get("spread_close"))
            ):
                update["close_bid"] = row.get("close") - row.get("spread_close") / 2
            if (
                "close_ask" in frame.columns
                and pd.isna(row.get("close_ask"))
                and not pd.isna(row.get("close"))
                and not pd.isna(row.get("spread_close"))
            ):
                update["close_ask"] = row.get("close") + row.get("spread_close") / 2
            if (
                "close" in frame.columns
                and pd.isna(row.get("close"))
                and not pd.isna(row.get("close_bid"))
                and not pd.isna(row.get("close_ask"))
            ):
                update["close"] = (row.get("close_bid") + row.get("close_ask")) / 2

            if update:
                for key, value in update.items():
                    frame.at[ts, key] = value
                updates.append((asset_id, ts, update))
        if not updates:
            return 0

        for asset_id_value, ts_value, update in updates:
            self._apply_update(table_cfg, asset_id_value, ts_value, update)
        self.log.info(
            "Tabla %s asset %s: %d campos derivados",
            table_cfg.table,
            asset_id,
            len(updates),
        )
        return len(updates)

    def _apply_update(
        self, table_cfg: TableConfig, asset_id: Any, ts: dt.datetime, update: Dict[str, Any]
    ) -> None:
        set_clause = ", ".join(f"{col} = %s" for col in update.keys())
        values = list(update.values())
        values.extend([asset_id, ts])
        sql = (
            f"UPDATE {self.cfg.schema}.{table_cfg.table} "
            f"SET {set_clause} "
            f"WHERE {table_cfg.asset_column} = %s AND {table_cfg.datetime_column} = %s"
        )
        self.db.execute(sql, values)


class Refiller:
    def __init__(self, db: Database, cfg: AppConfig, assets: AssetResolver):
        self.db = db
        self.cfg = cfg
        self.assets = assets
        self.log = logging.getLogger("completitud.refill")
        self.session = requests.Session()
        self.flags_path = cfg.output_dir / "refill_flags.json"
        self.flags: Dict[str, int] = {}
        self._load_flags()

    def _load_flags(self) -> None:
        if self.flags_path.exists():
            try:
                self.flags = json.loads(self.flags_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                self.flags = {}

    def _persist_flags(self) -> None:
        self.cfg.output_dir.mkdir(parents=True, exist_ok=True)
        self.flags_path.write_text(json.dumps(self.flags, indent=2, sort_keys=True), encoding="utf-8")

    def _flag_key(self, table: str, asset: Any, ts: str) -> str:
        return f"{table}:{asset}:{ts}"

    def should_skip(self, table: str, asset: Any, ts: str) -> bool:
        key = self._flag_key(table, asset, ts)
        return self.flags.get(key, 0) >= self.cfg.max_refill_attempts

    def mark_failure(self, table: str, asset: Any, ts: str) -> None:
        key = self._flag_key(table, asset, ts)
        self.flags[key] = self.flags.get(key, 0) + 1
        self._persist_flags()

    def clear_flag(self, table: str, asset: Any, ts: str) -> None:
        key = self._flag_key(table, asset, ts)
        if key in self.flags:
            del self.flags[key]
            self._persist_flags()

    def refill(self, analysis: GapAnalysisResult) -> Dict[str, Any]:
        actions: Dict[str, Any] = {}
        for table_cfg in self.cfg.timeseries_tables:
            table_actions: Dict[str, Any] = {}
            table_assets = analysis.tables.get(table_cfg.table, {})
            for asset_id, payload in table_assets.items():
                missing_dates = payload.get("missing_dates", [])
                field_gaps = payload.get("field_gaps", {})
                recovered = []
                if missing_dates:
                    recovered.extend(
                        self._refill_missing_rows(table_cfg, asset_id, missing_dates)
                    )
                if field_gaps:
                    recovered.extend(
                        self._refill_missing_fields(table_cfg, asset_id, field_gaps)
                    )
                if recovered:
                    table_actions[str(asset_id)] = recovered
            if table_actions:
                actions[table_cfg.table] = table_actions
        return actions

    def _refill_missing_rows(
        self, table_cfg: TableConfig, asset_id: Any, missing_dates: Sequence[str]
    ) -> List[str]:
        asset_meta = self.assets.get(asset_id)
        if not asset_meta:
            self.log.warning("Activo %s sin metadatos, no se rellena", asset_id)
            return []
        recovered: List[str] = []
        for iso_ts in missing_dates:
            if self.should_skip(table_cfg.table, asset_id, iso_ts):
                self.log.info(
                    "Saltando %s %s %s por exceder reintentos",
                    table_cfg.table,
                    asset_id,
                    iso_ts,
                )
                continue
            try:
                row = self._fetch_remote_row(table_cfg, asset_meta, iso_ts)
            except Exception as exc:
                self.log.error(
                    "Error obteniendo %s %s en %s: %s",
                    table_cfg.table,
                    asset_id,
                    iso_ts,
                    exc,
                )
                self.mark_failure(table_cfg.table, asset_id, iso_ts)
                continue
            if not row:
                self.log.warning(
                    "No se recibió dato remoto para %s %s en %s",
                    table_cfg.table,
                    asset_id,
                    iso_ts,
                )
                self.mark_failure(table_cfg.table, asset_id, iso_ts)
                continue
            self._insert_row(table_cfg, asset_id, iso_ts, row)
            recovered.append(f"row:{iso_ts}")
            self.clear_flag(table_cfg.table, asset_id, iso_ts)
        return recovered

    def _refill_missing_fields(
        self, table_cfg: TableConfig, asset_id: Any, field_gaps: Dict[str, int]
    ) -> List[str]:
        recovered: List[str] = []
        for field in field_gaps:
            try:
                count = self._refill_field(table_cfg, asset_id, field)
            except Exception as exc:
                self.log.error(
                    "Error rellenando campo %s.%s para asset %s: %s",
                    table_cfg.table,
                    field,
                    asset_id,
                    exc,
                )
                continue
            if count:
                recovered.append(f"field:{field}:{count}")
        return recovered

    def _insert_row(
        self, table_cfg: TableConfig, asset_id: Any, iso_ts: str, values: Dict[str, Any]
    ) -> None:
        timestamp = dt.datetime.fromisoformat(iso_ts)
        values.setdefault(table_cfg.asset_column, asset_id)
        values.setdefault(table_cfg.datetime_column, timestamp)
        columns = [table_cfg.asset_column, table_cfg.datetime_column]
        params = [asset_id, timestamp]
        for field, value in values.items():
            if field in columns:
                continue
            columns.append(field)
            params.append(value)
        placeholders = ", ".join(["%s"] * len(columns))
        conflict_cols = {table_cfg.asset_column, table_cfg.datetime_column}
        update_cols = [col for col in columns if col not in conflict_cols]
        if update_cols:
            update_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_cols)
            on_conflict = (
                f"ON CONFLICT ({table_cfg.asset_column}, {table_cfg.datetime_column}) DO UPDATE SET "
                f"{update_clause}"
            )
        else:
            on_conflict = f"ON CONFLICT ({table_cfg.asset_column}, {table_cfg.datetime_column}) DO NOTHING"
        sql = (
            f"INSERT INTO {self.cfg.schema}.{table_cfg.table} ({', '.join(columns)}) "
            f"VALUES ({placeholders}) "
            f"{on_conflict}"
        )
        self.db.execute(sql, params)

    def _refill_field(self, table_cfg: TableConfig, asset_id: Any, field: str) -> int:
        asset_meta = self.assets.get(asset_id)
        if not asset_meta:
            self.log.warning("Sin metadatos para %s al rellenar %s", asset_id, field)
            return 0
        sql = (
            f"SELECT {table_cfg.datetime_column} FROM {self.cfg.schema}.{table_cfg.table} "
            f"WHERE {table_cfg.asset_column} = %s AND ({field} IS NULL OR {field} = 0)"
        )
        rows = self.db.fetchall(sql, (asset_id,))
        updates = 0
        for row in rows:
            ts = row[table_cfg.datetime_column]
            iso_ts = normalize_datetime(ts, table_cfg.timezone).isoformat()
            if self.should_skip(table_cfg.table, asset_id, f"{field}:{iso_ts}"):
                continue
            try:
                remote = self._fetch_remote_row(table_cfg, asset_meta, iso_ts)
            except Exception as exc:
                self.log.error(
                    "Error remoto para campo %s en %s %s: %s",
                    field,
                    table_cfg.table,
                    asset_id,
                    exc,
                )
                self.mark_failure(table_cfg.table, asset_id, f"{field}:{iso_ts}")
                continue
            if remote and field in remote:
                sql_update = (
                    f"UPDATE {self.cfg.schema}.{table_cfg.table} SET {field} = %s "
                    f"WHERE {table_cfg.asset_column} = %s AND {table_cfg.datetime_column} = %s"
                )
                self.db.execute(sql_update, (remote[field], asset_id, ts))
                updates += 1
                self.clear_flag(table_cfg.table, asset_id, f"{field}:{iso_ts}")
        return updates

    def _fetch_remote_row(
        self, table_cfg: TableConfig, asset_meta: Optional[Dict[str, Any]], iso_ts: str
    ) -> Dict[str, Any]:
        if table_cfg.provider == "capital":
            return self._fetch_capital(table_cfg, asset_meta, iso_ts)
        if table_cfg.provider == "fmp":
            return self._fetch_fmp(table_cfg, asset_meta, iso_ts)
        if table_cfg.provider == "yfinance":
            return self._fetch_yfinance(table_cfg, asset_meta, iso_ts)
        raise ValueError(f"Proveedor desconocido: {table_cfg.provider}")

    def _fetch_capital(
        self, table_cfg: TableConfig, asset_meta: Optional[Dict[str, Any]], iso_ts: str
    ) -> Dict[str, Any]:
        if not self.cfg.capital_api_key or not self.cfg.capital_api_url:
            raise RuntimeError("Capital.com no configurado")
        symbol = (asset_meta or {}).get("symbol")
        if not symbol:
            raise RuntimeError("Activo sin símbolo para Capital.com")
        endpoint = table_cfg.endpoint or f"/prices/{symbol}"
        url = self.cfg.capital_api_url.rstrip("/") + endpoint
        params = {
            "from": iso_ts,
            "to": iso_ts,
            "max": 1,
        }
        headers = {"X-API-KEY": self.cfg.capital_api_key}
        response = self.session.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        if not data:
            return {}
        record = data[0] if isinstance(data, list) else data.get("prices", [{}])[0]
        mapped = map_remote_record(record, table_cfg.field_map or CAPITAL_DEFAULT_FIELD_MAP)
        return self._normalise_remote_record(mapped)

    def _fetch_fmp(
        self, table_cfg: TableConfig, asset_meta: Optional[Dict[str, Any]], iso_ts: str
    ) -> Dict[str, Any]:
        if not self.cfg.fmp_api_key:
            raise RuntimeError("FMP API key no configurada")
        symbol = (asset_meta or {}).get("symbol")
        if not symbol:
            raise RuntimeError("Activo sin símbolo para FMP")
        base = self.cfg.fmp_api_url.rstrip("/")
        if table_cfg.frequency == "daily":
            endpoint = f"/historical-price-full/{symbol}"
            params = {"from": iso_ts[:10], "to": iso_ts[:10], "apikey": self.cfg.fmp_api_key}
        else:
            interval = table_cfg.interval_minutes or 5
            endpoint = f"/historical-chart/{interval}min/{symbol}"
            params = {"from": iso_ts, "to": iso_ts, "apikey": self.cfg.fmp_api_key}
        url = base + endpoint
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if not payload:
            return {}
        if "historical" in payload:
            records = payload["historical"]
        else:
            records = payload
        if not records:
            return {}
        record = records[0]
        field_map = table_cfg.field_map or (
            FMP_DAILY_FIELD_MAP if table_cfg.frequency == "daily" else FMP_INTRADAY_FIELD_MAP
        )
        mapped = map_remote_record(record, field_map)
        return self._normalise_remote_record(mapped)

    def _fetch_yfinance(
        self, table_cfg: TableConfig, asset_meta: Optional[Dict[str, Any]], iso_ts: str
    ) -> Dict[str, Any]:
        if not self.cfg.yfinance_enabled or not HAS_YFINANCE:
            raise RuntimeError("yfinance no disponible")
        symbol = (asset_meta or {}).get("symbol")
        if not symbol:
            raise RuntimeError("Activo sin símbolo para Yahoo Finance")
        ticker = yf.Ticker(symbol)
        if table_cfg.frequency == "daily":
            day = dt.date.fromisoformat(iso_ts[:10])
            end_day = day + dt.timedelta(days=1)
            history = ticker.history(start=day.isoformat(), end=end_day.isoformat())
        else:
            dt_from = dt.datetime.fromisoformat(iso_ts)
            dt_to = dt_from + dt.timedelta(minutes=table_cfg.interval_minutes or 5)
            history = ticker.history(
                interval=f"{table_cfg.interval_minutes or 5}m",
                start=dt_from.isoformat(),
                end=dt_to.isoformat(),
            )
        if history.empty:
            return {}
        row = history.iloc[0]
        record = {key: row.get(remote) for key, remote in YFINANCE_FIELD_MAP.items()}
        return self._normalise_remote_record(record)

    def _normalise_remote_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        normalised: Dict[str, Any] = {}
        for key, value in record.items():
            if isinstance(value, str):
                value = value.strip()
            if isinstance(value, Decimal):
                value = float(value)
            if key in NUMERIC_FIELDS and isinstance(value, str):
                try:
                    normalised[key] = float(value)
                except ValueError:
                    continue
            elif key in INTEGER_FIELDS and value is not None:
                normalised[key] = int(value)
            else:
                normalised[key] = value
        if "open_bid" in normalised and "open_ask" in normalised and "spread_open" not in normalised:
            try:
                normalised["spread_open"] = normalised["open_ask"] - normalised["open_bid"]
            except TypeError:
                pass
        if "close_bid" in normalised and "close_ask" in normalised and "spread_close" not in normalised:
            try:
                normalised["spread_close"] = normalised["close_ask"] - normalised["close_bid"]
            except TypeError:
                pass
        return normalised


class AuditTrail:
    def __init__(self, cfg: AppConfig):
        self.cfg = cfg
        self.log = logging.getLogger("completitud.audit")

    def persist(self, analysis: GapAnalysisResult, refill_actions: Dict[str, Any]) -> None:
        payload = {
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
            "config_hash": self._config_hash(),
            "analysis": analysis.to_json(),
            "refill": refill_actions,
        }
        self.cfg.output_dir.mkdir(parents=True, exist_ok=True)
        filename = self.cfg.output_dir / f"auditoria_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filename.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        self.log.info("Audit trail guardado en %s", filename)

    def _config_hash(self) -> str:
        raw = json.dumps([asdict(t) for t in self.cfg.timeseries_tables], sort_keys=True).encode("utf-8")
        return sha256(raw).hexdigest()


def summarise_statistics(analysis: GapAnalysisResult) -> Dict[str, Any]:
    totals = {
        "assets": 0,
        "rows": 0,
        "missing_dates": 0,
        "field_gaps": 0,
        "derived_updates": 0,
    }
    per_asset: Dict[str, Dict[str, Any]] = {}
    for table_name, assets in analysis.tables.items():
        for asset_id, payload in assets.items():
            totals["assets"] += 1
            totals["rows"] += payload.get("rows", 0)
            totals["missing_dates"] += len(payload.get("missing_dates", []))
            totals["field_gaps"] += sum(payload.get("field_gaps", {}).values())
            totals["derived_updates"] += payload.get("derived_updates", 0)
            key = f"{table_name}:{asset_id}"
            per_asset[key] = {
                "rows": payload.get("rows", 0),
                "missing_dates": len(payload.get("missing_dates", [])),
                "field_gaps": sum(payload.get("field_gaps", {}).values()),
            }
    totals["assets_with_gaps"] = sum(1 for asset in per_asset.values() if asset["missing_dates"] or asset["field_gaps"])
    totals["gap_ratio"] = (
        totals["assets_with_gaps"] / totals["assets"] * 100 if totals["assets"] else 0
    )
    return {"totals": totals, "per_asset": per_asset}


def save_analysis(cfg: AppConfig, analysis: GapAnalysisResult) -> Path:
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    path = cfg.output_dir / "completitud.json"
    path.write_text(json.dumps(analysis.to_json(), indent=2, sort_keys=True), encoding="utf-8")
    logging.getLogger("completitud").info("Informe JSON escrito en %s", path)
    return path


def print_summary(stats: Dict[str, Any]) -> None:
    totals = stats["totals"]
    logging.getLogger("completitud").info(
        "Activos analizados: %d | Huecos detectados: %d fechas, %d campos | %% activos con huecos: %.2f",
        totals["assets"],
        totals["missing_dates"],
        totals["field_gaps"],
        totals["gap_ratio"],
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auditoría de completitud de datos financieros")
    parser.add_argument(
        "--sin-relleno",
        action="store_true",
        help="No intentar completar datos faltantes",
    )
    parser.add_argument(
        "--max-activos",
        type=int,
        default=None,
        help="Limitar el número de activos analizados por tabla",
    )
    parser.add_argument(
        "--env",
        type=str,
        default=None,
        help="Ruta a un archivo .env alternativo",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    cfg = AppConfig.from_env(Path(args.env) if args.env else None)
    setup_logging(cfg)
    log = logging.getLogger("completitud")
    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    try:
        with Database(cfg) as db:
            inspector = SchemaInspector(db, cfg)
            inspector.report()
            assets = AssetResolver(db, cfg)
            assets.load()

            analyzer = TimeseriesAnalyzer(db, cfg, assets, max_assets=args.max_activos)
            analysis = analyzer.run()

            save_analysis(cfg, analysis)
            stats = summarise_statistics(analysis)
            print_summary(stats)

            if not args.sin_relleno:
                refiller = Refiller(db, cfg, assets)
                refill_actions = refiller.refill(analysis)
            else:
                refill_actions = {}
                log.info("Modo sin relleno activado, no se completan huecos")

            audit = AuditTrail(cfg)
            audit.persist(analysis, refill_actions)
    except psycopg2.OperationalError as exc:
        log.error("Conexión a la base de datos fallida: %s", exc)
        log.error("Revise credenciales y disponibilidad del servidor")
        return

    log.info("Proceso completado")


if __name__ == "__main__":
    main()
