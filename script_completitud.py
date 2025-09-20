"""Herramienta integral de auditoría y completitud de datos financieros.

Este script inspecciona la base de datos PostgreSQL indicada en la configuración
(.env), detecta elementos huérfanos en el esquema, analiza la completitud de las
series temporales de cotizaciones y, cuando es posible, completa los huecos
consultando las API de Capital.com (para datos CFD) y Financial Modeling Prep
(FMP) o Yahoo Finance como respaldo. Todas las operaciones quedan registradas en
tablas de auditoría con trazabilidad completa.

Características principales
---------------------------
* Carga de credenciales y parámetros desde un archivo .env.
* Descubrimiento de tablas, vistas y claves foráneas para localizar restos de
  borrados u orfandades.
* Validación semántica de los campos financieros: precios coherentes, volúmenes
  no negativos, alineación de máximos/mínimos, etc.
* Detección de huecos (NULL, NaN, 0 sospechoso, strings vacíos, placeholders) y
  fechas faltantes por activo.
* Recuperación automática de datos faltantes usando Capital.com o FMP/YFinance,
  respetando límites de reintentos y marcando flags persistentes cuando no se
  logra completar la información tras tres intentos.
* Registro de auditoría con hash de configuración y resumen de acciones
  ejecutadas.
* Informes en JSON y en texto de los hallazgos.

Requisitos
----------
Dependencias sugeridas (instalarlas vía pip):

```
pip install python-dotenv psycopg2-binary pandas requests tqdm yfinance
```

La librería `yfinance` es opcional; si no está instalada, el script continúa sin
la fuente de respaldo.

Uso
----

1. Crear un archivo `.env` en el mismo directorio con las variables necesarias.
   Ver `AppConfig.from_env` para el detalle completo.
2. Ejecutar el script:

```
python script_completitud.py
```

Opcionalmente se pueden indicar parámetros adicionales mediante argumentos de
línea de comandos (`--reanalizar` para forzar reanálisis final, `--max-activos`
para limitar el número de activos auditados, etc.).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import math
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple

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


NUMERIC_COLUMNS = {
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

INTEGER_COLUMNS = {
    "ts_epoch",
}

CAPITAL_FIELD_MAP = {
    "open": "openPrice",
    "high": "highPrice",
    "low": "lowPrice",
    "close": "closePrice",
    "volume": "lastTradedVolume",
    "spread_open": "openPrice.spread",
    "spread_close": "closePrice.spread",
    "open_bid": "openPrice.bid",
    "open_ask": "openPrice.ask",
    "close_bid": "closePrice.bid",
    "close_ask": "closePrice.ask",
}

FMP_DAILY_FIELD_MAP = {
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "divadj_open": "adjOpen",
    "divadj_high": "adjHigh",
    "divadj_low": "adjLow",
    "divadj_close": "adjClose",
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

YF_FIELD_MAP = {
    "open": "Open",
    "high": "High",
    "low": "Low",
    "close": "Close",
    "volume": "Volume",
}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "si", "sí"}


def _json_env(name: str) -> Optional[Any]:
    raw = os.getenv(name)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logging.getLogger(__name__).warning("No se pudo parsear %s como JSON", name)
        return None


@dataclass
class TimeseriesTableConfig:
    """Configuración de una tabla de series temporales a auditar."""

    table: str
    asset_column: str
    datetime_column: str
    data_columns: List[str]
    frequency: str = "daily"  # daily | intraday
    interval_minutes: Optional[int] = None
    timezone: str = "UTC"
    allow_weekends: bool = False
    conflict_columns: Sequence[str] = field(default_factory=list)
    symbol_column: Optional[str] = None
    price_floor: Optional[float] = None
    price_ceiling: Optional[float] = None

    def expected_timedelta(self) -> Optional[pd.Timedelta]:
        if self.frequency == "daily":
            return pd.Timedelta(days=1)
        if self.frequency == "intraday" and self.interval_minutes:
            return pd.Timedelta(minutes=self.interval_minutes)
        return None


@dataclass
class AppConfig:
    """Parámetros globales cargados desde .env."""

    pg_dsn: str
    capital_api_key: Optional[str]
    capital_email: Optional[str]
    capital_password: Optional[str]
    fmp_api_key: Optional[str]
    max_requests_per_minute: int = 20
    request_timeout: int = 30
    request_retries: int = 3
    tables: List[TimeseriesTableConfig] = field(default_factory=list)
    reports_dir: Path = Path("reports")
    logs_dir: Path = Path("logs")
    enable_refill: bool = True
    asset_lookup_query: Optional[str] = None
    audit_table: str = "data_completeness_audit"
    flags_table: str = "data_completeness_flags"
    chunk_size: int = 5000
    max_assets: Optional[int] = None
    timezone: str = "UTC"
    reanalyse_after_fill: bool = True

    @staticmethod
    def _default_tables() -> List[TimeseriesTableConfig]:
        return [
            TimeseriesTableConfig(
                table="cotizaciones_diarias_cfd",
                asset_column="asset_id",
                datetime_column="fecha",
                data_columns=["open", "high", "low", "close", "volume"],
                frequency="daily",
                conflict_columns=["asset_id", "fecha"],
            ),
            TimeseriesTableConfig(
                table="cotizaciones_intradia_cfd",
                asset_column="asset_id",
                datetime_column="timestamp",
                data_columns=["open", "high", "low", "close", "volume"],
                frequency="intraday",
                interval_minutes=1,
                conflict_columns=["asset_id", "timestamp"],
            ),
            TimeseriesTableConfig(
                table="cotizaciones_diarias",
                asset_column="asset_id",
                datetime_column="fecha",
                data_columns=["open", "high", "low", "close", "volume"],
                frequency="daily",
                conflict_columns=["asset_id", "fecha"],
            ),
            TimeseriesTableConfig(
                table="cotizaciones_intradia",
                asset_column="asset_id",
                datetime_column="timestamp",
                data_columns=["open", "high", "low", "close", "volume"],
                frequency="intraday",
                interval_minutes=1,
                conflict_columns=["asset_id", "timestamp"],
            ),
        ]

    @classmethod
    def from_env(cls) -> "AppConfig":
        load_dotenv()

        pg_dsn = os.getenv("PG_DSN")
        if not pg_dsn:
            host = os.getenv("PG_HOST", "localhost")
            port = os.getenv("PG_PORT", "5432")
            user = os.getenv("PG_USER", "postgres")
            password = os.getenv("PG_PASSWORD", "")
            database = os.getenv("PG_DATABASE", "postgres")
            pg_dsn = f"host={host} port={port} user={user} password={password} dbname={database}"

        tables = cls._default_tables()
        override = _json_env("TIMESERIES_TABLES_JSON")
        if override:
            tables = []
            for item in override:
                tables.append(
                    TimeseriesTableConfig(
                        table=item["table"],
                        asset_column=item.get("asset_column", "asset_id"),
                        datetime_column=item.get("datetime_column", "fecha"),
                        data_columns=item.get("data_columns", ["open", "high", "low", "close", "volume"]),
                        frequency=item.get("frequency", "daily"),
                        interval_minutes=item.get("interval_minutes"),
                        timezone=item.get("timezone", "UTC"),
                        allow_weekends=item.get("allow_weekends", False),
                        conflict_columns=item.get("conflict_columns", [item.get("asset_column", "asset_id"), item.get("datetime_column", "fecha")]),
                        symbol_column=item.get("symbol_column"),
                        price_floor=item.get("price_floor"),
                        price_ceiling=item.get("price_ceiling"),
                    )
                )

        return cls(
            pg_dsn=pg_dsn,
            capital_api_key=os.getenv("CAPITAL_API_KEY"),
            capital_email=os.getenv("CAPITAL_EMAIL"),
            capital_password=os.getenv("CAPITAL_PASSWORD"),
            fmp_api_key=os.getenv("FMP_API_KEY"),
            max_requests_per_minute=int(os.getenv("MAX_REQUESTS_PER_MIN", "20")),
            request_timeout=int(os.getenv("REQUEST_TIMEOUT", "30")),
            request_retries=int(os.getenv("REQUEST_RETRIES", "3")),
            tables=tables,
            reports_dir=Path(os.getenv("REPORTS_DIR", "reports")),
            logs_dir=Path(os.getenv("LOGS_DIR", "logs")),
            enable_refill=_env_bool("ENABLE_REFILL", True),
            asset_lookup_query=os.getenv("ASSET_LOOKUP_QUERY"),
            audit_table=os.getenv("AUDIT_TABLE", "data_completeness_audit"),
            flags_table=os.getenv("FLAGS_TABLE", "data_completeness_flags"),
            chunk_size=int(os.getenv("DB_CHUNK_SIZE", "5000")),
            max_assets=int(os.getenv("MAX_ASSETS")) if os.getenv("MAX_ASSETS") else None,
            timezone=os.getenv("DEFAULT_TIMEZONE", "UTC"),
            reanalyse_after_fill=_env_bool("REANALYSE_AFTER_FILL", True),
        )

def setup_logging(cfg: AppConfig) -> None:
    cfg.logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = cfg.logs_dir / f"completitud_{dt.datetime.utcnow().strftime('%Y%m%d')}.log"
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])

class CapitalComClient:
    BASE_URL = "https://api-capital.backend-capital.com/api/v1"

    def __init__(self, cfg: AppConfig, logger: logging.Logger) -> None:
        self.cfg = cfg
        self.logger = logger.getChild("CapitalCom")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        if cfg.capital_api_key:
            self.session.headers["X-CAP-API-KEY"] = cfg.capital_api_key
        self.cst = None
        self.xst = None
        self.last_request = 0.0
        self.login()

    def login(self) -> None:
        if not (self.cfg.capital_email and self.cfg.capital_password):
            self.logger.warning("Credenciales de Capital.com incompletas. Se omiten peticiones.")
            return

        payload = {
            "identifier": self.cfg.capital_email,
            "password": self.cfg.capital_password,
        }
        resp = self.session.post(f"{self.BASE_URL}/session", json=payload, timeout=self.cfg.request_timeout)
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"No fue posible autenticar en Capital.com: {resp.status_code} {resp.text}")

        self.cst = resp.headers.get("CST")
        self.xst = resp.headers.get("X-SECURITY-TOKEN") or resp.headers.get("X-SECURITY-TOKEN".lower())
        if not self.cst or not self.xst:
            try:
                body = resp.json()
            except Exception:  # pragma: no cover
                body = {}
            self.cst = self.cst or body.get("CST")
            self.xst = self.xst or body.get("securityToken") or body.get("X-SECURITY-TOKEN")
        if not self.cst or not self.xst:
            raise RuntimeError("La autenticación de Capital.com no entregó tokens CST/XST")

        self.session.headers.update({"CST": self.cst, "X-SECURITY-TOKEN": self.xst})
        self.logger.info("Sesión Capital.com iniciada correctamente")

    def _respect_rate_limit(self) -> None:
        min_interval = 60.0 / max(self.cfg.max_requests_per_minute, 1)
        elapsed = time.monotonic() - self.last_request
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

    def fetch_prices(
        self,
        epic: str,
        start: dt.datetime,
        end: dt.datetime,
        resolution: str,
    ) -> List[Dict[str, Any]]:
        if not self.cst or not self.xst:
            self.logger.debug("Sin tokens de sesión, no se solicitarán precios Capital.com")
            return []

        params = {
            "resolution": resolution,
            "from": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "to": end.strftime("%Y-%m-%dT%H:%M:%S"),
            "pageSize": 2000,
        }

        path = f"{self.BASE_URL}/prices/{epic}"
        records: List[Dict[str, Any]] = []
        for attempt in range(self.cfg.request_retries):
            try:
                self._respect_rate_limit()
                resp = self.session.get(path, params=params, timeout=self.cfg.request_timeout)
                self.last_request = time.monotonic()
                if resp.status_code != 200:
                    raise RuntimeError(f"Capital.com status {resp.status_code}: {resp.text}")
                data = resp.json()
                prices = data.get("prices", [])
                if isinstance(prices, list):
                    records.extend([item for item in prices if isinstance(item, dict)])
                break
            except Exception as exc:  # pragma: no cover
                self.logger.warning("Intento %s fallido al pedir precios Capital.com: %s", attempt + 1, exc)
                time.sleep(2 * (attempt + 1))
        return records

class FMPClient:
    BASE_URL = "https://financialmodelingprep.com/api/v3"

    def __init__(self, cfg: AppConfig, logger: logging.Logger) -> None:
        self.cfg = cfg
        self.logger = logger.getChild("FMP")
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def fetch_daily(self, symbol: str, start: dt.date, end: dt.date) -> List[Dict[str, Any]]:
        if not self.cfg.fmp_api_key:
            self.logger.warning("Sin API key de FMP, no se consultarán datos diarios")
            return []
        url = f"{self.BASE_URL}/historical-price-full/{symbol.upper()}"
        params = {
            "from": start.strftime("%Y-%m-%d"),
            "to": end.strftime("%Y-%m-%d"),
            "apikey": self.cfg.fmp_api_key,
        }
        try:
            resp = self.session.get(url, params=params, timeout=self.cfg.request_timeout)
            if resp.status_code != 200:
                raise RuntimeError(f"FMP status {resp.status_code}: {resp.text}")
            data = resp.json()
        except Exception as exc:  # pragma: no cover
            self.logger.warning("Error al consultar FMP diario para %s: %s", symbol, exc)
            return []
        items = data.get("historical") if isinstance(data, dict) else None
        return [item for item in items or [] if isinstance(item, dict)]

    def fetch_intraday(self, symbol: str, minutes: int, start: dt.datetime, end: dt.datetime) -> List[Dict[str, Any]]:
        if not self.cfg.fmp_api_key:
            self.logger.warning("Sin API key de FMP, no se consultarán datos intradía")
            return []
        interval = f"{minutes}min"
        url = f"{self.BASE_URL}/historical-chart/{interval}/{symbol.upper()}"
        params = {
            "apikey": self.cfg.fmp_api_key,
            "from": start.strftime("%Y-%m-%d"),
            "to": end.strftime("%Y-%m-%d"),
        }
        try:
            resp = self.session.get(url, params=params, timeout=self.cfg.request_timeout)
            if resp.status_code != 200:
                raise RuntimeError(f"FMP status {resp.status_code}: {resp.text}")
            data = resp.json()
        except Exception as exc:  # pragma: no cover
            self.logger.warning("Error al consultar FMP intradía para %s: %s", symbol, exc)
            return []
        return [item for item in data if isinstance(item, dict)]

class YahooFinanceClient:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger.getChild("YahooFinance")
        if not HAS_YFINANCE:
            self.logger.warning("yfinance no está instalado; no habrá respaldo de datos")

    def fetch(self, symbol: str, start: dt.datetime, end: dt.datetime, interval: str) -> List[Dict[str, Any]]:
        if not HAS_YFINANCE:
            return []
        try:
            df = yf.download(  # type: ignore[attr-defined]
                symbol,
                start=start,
                end=end + dt.timedelta(days=1),
                interval=interval,
                progress=False,
                auto_adjust=False,
            )
        except Exception as exc:  # pragma: no cover
            self.logger.warning("Yahoo Finance falló para %s: %s", symbol, exc)
            return []
        if df.empty:
            return []
        df = df.reset_index()
        records: List[Dict[str, Any]] = []
        for row in df.to_dict(orient="records"):
            timestamp = row.get("Datetime") or row.get("Date")
            if pd.isna(timestamp):
                continue
            records.append(
                {
                    "timestamp": pd.Timestamp(timestamp).to_pydatetime(),
                    "open": float(row.get("Open", float("nan"))),
                    "high": float(row.get("High", float("nan"))),
                    "low": float(row.get("Low", float("nan"))),
                    "close": float(row.get("Close", float("nan"))),
                    "volume": float(row.get("Volume", float("nan"))),
                }
            )
        return records

class DatabaseManager:
    def __init__(self, cfg: AppConfig, logger: logging.Logger) -> None:
        self.cfg = cfg
        self.logger = logger.getChild("DB")
        self.conn = psycopg2.connect(cfg.pg_dsn)
        self.conn.autocommit = False

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "DatabaseManager":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.close()

    def list_tables(self) -> List[Dict[str, Any]]:
        query = """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            return cur.fetchall()

    def list_views(self) -> List[Dict[str, Any]]:
        query = """
        SELECT table_schema, table_name
        FROM information_schema.views
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            return cur.fetchall()

    def list_foreign_keys(self) -> List[Dict[str, Any]]:
        query = """
        SELECT
            tc.table_schema,
            tc.table_name,
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        ORDER BY tc.table_schema, tc.table_name
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            return cur.fetchall()

    def count_orphans(self, schema: str, table: str, column: str, parent_schema: str, parent_table: str, parent_column: str) -> int:
        query = f"""
        SELECT COUNT(*)
        FROM {schema}.{table} child
        LEFT JOIN {parent_schema}.{parent_table} parent
            ON child.{column} = parent.{parent_column}
        WHERE child.{column} IS NOT NULL AND parent.{parent_column} IS NULL
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            return int(result[0]) if result else 0

    def fetch_distinct_assets(self, table: str, column: str, limit: Optional[int]) -> List[Any]:
        query = f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL ORDER BY 1"
        if limit:
            query += f" LIMIT {int(limit)}"
        with self.conn.cursor() as cur:
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]

    def fetch_rows_for_asset(self, table: str, asset_column: str, asset_id: Any, datetime_column: str) -> Iterator[Dict[str, Any]]:
        cursor_name = f"cur_{table.replace('.', '_')}_{asset_id}"
        cur = self.conn.cursor(name=cursor_name, cursor_factory=psycopg2.extras.RealDictCursor)
        cur.itersize = self.cfg.chunk_size
        query = f"SELECT * FROM {table} WHERE {asset_column} = %s ORDER BY {datetime_column}"
        cur.execute(query, (asset_id,))
        for row in cur:
            yield dict(row)
        cur.close()

    def fetch_row(
        self,
        table: str,
        asset_column: str,
        asset_id: Any,
        datetime_column: str,
        dt_value: dt.datetime,
    ) -> Optional[Dict[str, Any]]:
        query = f"SELECT * FROM {table} WHERE {asset_column} = %s AND {datetime_column} = %s LIMIT 1"
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (asset_id, dt_value))
            row = cur.fetchone()
            return dict(row) if row else None

    def update_row(
        self,
        table: str,
        asset_column: str,
        asset_id: Any,
        datetime_column: str,
        dt_value: dt.datetime,
        updates: Dict[str, Any],
    ) -> bool:
        if not updates:
            return False
        assignments = []
        values: List[Any] = []
        for column, value in updates.items():
            assignments.append(f"{column} = %s")
            values.append(value)
        values.extend([asset_id, dt_value])
        sql = f"UPDATE {table} SET {', '.join(assignments)} WHERE {asset_column} = %s AND {datetime_column} = %s"
        with self.conn.cursor() as cur:
            cur.execute(sql, values)
            return cur.rowcount > 0

    def execute(self, query: str, params: Optional[Sequence[Any]] = None) -> None:
        with self.conn.cursor() as cur:
            cur.execute(query, params)

    def executemany(self, query: str, params_seq: Sequence[Sequence[Any]]) -> None:
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, query, params_seq)

    def fetch_asset_metadata(self, query: str) -> Dict[Any, Dict[str, Any]]:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            return {row.get("asset_id") or row.get("id"): dict(row) for row in cur.fetchall()}

    def ensure_tables(self, audit_table: str, flags_table: str) -> None:
        audit_sql = f"""
        CREATE TABLE IF NOT EXISTS {audit_table} (
            id SERIAL PRIMARY KEY,
            executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            config_hash TEXT NOT NULL,
            report JSONB NOT NULL
        )
        """
        flags_sql = f"""
        CREATE TABLE IF NOT EXISTS {flags_table} (
            id SERIAL PRIMARY KEY,
            table_name TEXT NOT NULL,
            asset_id TEXT,
            datetime_value TIMESTAMPTZ,
            failure_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (table_name, asset_id, datetime_value)
        )
        """
        self.execute(audit_sql)
        self.execute(flags_sql)

    def get_flag(self, table: str, asset_id: Any, dt_value: dt.datetime) -> Optional[Dict[str, Any]]:
        query = f"""
        SELECT * FROM {self.cfg.flags_table}
        WHERE table_name = %s AND asset_id = %s AND datetime_value = %s
        """
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (table, str(asset_id), dt_value))
            row = cur.fetchone()
            return dict(row) if row else None

    def upsert_flag(self, table: str, asset_id: Any, dt_value: dt.datetime, error: str) -> None:
        query = f"""
        INSERT INTO {self.cfg.flags_table} (table_name, asset_id, datetime_value, failure_count, last_error, last_attempt_at)
        VALUES (%s, %s, %s, 1, %s, NOW())
        ON CONFLICT (table_name, asset_id, datetime_value)
        DO UPDATE SET failure_count = {self.cfg.flags_table}.failure_count + 1, last_error = EXCLUDED.last_error, last_attempt_at = NOW()
        """
        self.execute(query, (table, str(asset_id), dt_value, error[:500]))

    def clear_flag(self, table: str, asset_id: Any, dt_value: dt.datetime) -> None:
        query = f"DELETE FROM {self.cfg.flags_table} WHERE table_name = %s AND asset_id = %s AND datetime_value = %s"
        self.execute(query, (table, str(asset_id), dt_value))

    def insert_rows(self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]], conflict_columns: Sequence[str]) -> None:
        if not rows:
            return
        cols = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        conflict = ", ".join(conflict_columns)
        update_assignments = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns])
        sql = f"""
        INSERT INTO {table} ({cols}) VALUES ({placeholders})
        ON CONFLICT ({conflict}) DO UPDATE SET {update_assignments}
        """
        self.executemany(sql, rows)

    def list_flags(self) -> List[Dict[str, Any]]:
        query = f"SELECT table_name, asset_id, datetime_value, failure_count, last_error, last_attempt_at FROM {self.cfg.flags_table}"
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                return [dict(row) for row in cur.fetchall()]
        except Exception:
            return []

def is_placeholder(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() in PLACEHOLDER_VALUES:
        return True
    if isinstance(value, (float, Decimal)) and (pd.isna(value) or math.isnan(float(value))):
        return True
    return False


def is_missing_value(value: Any) -> bool:
    return is_placeholder(value)


def as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (float, int, Decimal)):
        return float(value)
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
        try:
            return float(value)
        except ValueError:
            return None
    return None


def is_business_day(ts: pd.Timestamp) -> bool:
    return bool(ts.weekday() < 5)


def compress_missing_dates(dates: List[pd.Timestamp]) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    if not dates:
        return []
    dates = sorted(dates)
    ranges: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    start = dates[0]
    prev = dates[0]
    for current in dates[1:]:
        if current - prev > pd.Timedelta(days=1):
            ranges.append((start, prev))
            start = current
        prev = current
    ranges.append((start, prev))
    return ranges

class DataCompletenessAuditor:
    def __init__(
        self,
        cfg: AppConfig,
        db: DatabaseManager,
        capital: CapitalComClient,
        fmp: FMPClient,
        yf_client: YahooFinanceClient,
    ) -> None:
        self.cfg = cfg
        self.db = db
        self.capital = capital
        self.fmp = fmp
        self.yf_client = yf_client
        self.logger = logging.getLogger("DataCompletenessAuditor")
        self.asset_metadata: Dict[Any, Dict[str, Any]] = {}
        if cfg.asset_lookup_query:
            try:
                self.asset_metadata = db.fetch_asset_metadata(cfg.asset_lookup_query)
            except Exception as exc:
                self.logger.warning("No se pudo obtener metadata de activos: %s", exc)

    def inspect_schema(self) -> Dict[str, Any]:
        tables = self.db.list_tables()
        views = self.db.list_views()
        fks = self.db.list_foreign_keys()
        orphan_info = []
        for fk in fks:
            try:
                count = self.db.count_orphans(
                    schema=fk["table_schema"],
                    table=fk["table_name"],
                    column=fk["column_name"],
                    parent_schema=fk["foreign_table_schema"],
                    parent_table=fk["foreign_table_name"],
                    parent_column=fk["foreign_column_name"],
                )
            except Exception as exc:
                self.logger.warning("Fallo al contar huérfanos en %s.%s: %s", fk["table_name"], fk["column_name"], exc)
                continue
            if count > 0:
                orphan_info.append(
                    {
                        "tabla": f"{fk['table_schema']}.{fk['table_name']}",
                        "columna": fk["column_name"],
                        "padre": f"{fk['foreign_table_schema']}.{fk['foreign_table_name']}",
                        "columna_padre": fk["foreign_column_name"],
                        "registros_huerfanos": count,
                    }
                )

        return {
            "tablas": tables,
            "vistas": views,
            "foreign_keys": fks,
            "elementos_huerfanos": orphan_info,
        }

    def analyse_table(self, cfg: TimeseriesTableConfig) -> Dict[str, Any]:
        self.logger.info("Analizando %s", cfg.table)
        assets = self.db.fetch_distinct_assets(cfg.table, cfg.asset_column, self.cfg.max_assets)
        null_query = f"SELECT COUNT(*) FROM {cfg.table} WHERE {cfg.asset_column} IS NULL"
        try:
            with self.db.conn.cursor() as cur:
                cur.execute(null_query)
                null_assets = int(cur.fetchone()[0])
        except Exception:
            null_assets = 0
        report: Dict[str, Any] = {
            "table": cfg.table,
            "asset_column": cfg.asset_column,
            "datetime_column": cfg.datetime_column,
            "assets": {},
            "sin_asset_id": null_assets,
        }
        for asset_id in assets:
            asset_report = self.analyse_asset(cfg, asset_id)
            report["assets"][str(asset_id)] = asset_report
        return report

    def analyse_asset(self, cfg: TimeseriesTableConfig, asset_id: Any) -> Dict[str, Any]:
        issues: Dict[str, Any] = {
            "missing_rows": [],
            "invalid_rows": [],
            "missing_dates": [],
            "stats": {
                "rows": 0,
                "missing_field_rows": 0,
                "invalid_rows": 0,
            },
            "metadata": {},
        }

        rows: List[Dict[str, Any]] = []
        for row in self.db.fetch_rows_for_asset(cfg.table, cfg.asset_column, asset_id, cfg.datetime_column):
            rows.append(row)

        if not rows:
            return issues

        df = pd.DataFrame(rows)
        if cfg.datetime_column not in df.columns:
            self.logger.warning("La tabla %s no contiene la columna temporal %s", cfg.table, cfg.datetime_column)
            return issues

        df[cfg.datetime_column] = pd.to_datetime(df[cfg.datetime_column])
        df = df.sort_values(cfg.datetime_column).reset_index(drop=True)
        issues["stats"]["rows"] = len(df)
        sample_row = df.iloc[0].to_dict()
        issues["metadata"] = {
            "asset_id": asset_id,
            "symbol": sample_row.get("symbol") if "symbol" in sample_row else None,
            "epic": sample_row.get("epic") if "epic" in sample_row else None,
        }
        if cfg.symbol_column and cfg.symbol_column in sample_row:
            issues["metadata"]["symbol"] = sample_row.get(cfg.symbol_column)

        missing_dates: List[pd.Timestamp] = []

        for idx, row in df.iterrows():
            timestamp = row[cfg.datetime_column]
            row_missing = False
            row_invalid = False

            for col in cfg.data_columns:
                if col not in row:
                    continue
                value = row[col]
                if is_placeholder(value):
                    row_missing = True
                    continue
                numeric_value = as_float(value)
                if numeric_value is None:
                    row_invalid = True
                    continue
                if col in {"open", "high", "low", "close"} and numeric_value < 0:
                    row_invalid = True
                if col == "volume" and numeric_value <= 0:
                    row_invalid = True
                if cfg.price_floor is not None and numeric_value < cfg.price_floor:
                    row_invalid = True
                if cfg.price_ceiling is not None and numeric_value > cfg.price_ceiling:
                    row_invalid = True

            open_px = as_float(row.get("open"))
            high_px = as_float(row.get("high"))
            low_px = as_float(row.get("low"))
            close_px = as_float(row.get("close"))
            if None not in (open_px, high_px, low_px, close_px):
                assert open_px is not None and high_px is not None and low_px is not None and close_px is not None
                if open_px > high_px + 1e-9 or close_px > high_px + 1e-9:
                    row_invalid = True
                if low_px > open_px + 1e-9 or low_px > close_px + 1e-9:
                    row_invalid = True
                if high_px < low_px:
                    row_invalid = True

            if row_missing:
                issues["missing_rows"].append(
                    {
                        "index": int(idx),
                        "timestamp": timestamp.isoformat(),
                        "row": {col: row.get(col) for col in cfg.data_columns},
                    }
                )
            if row_invalid:
                issues["invalid_rows"].append(
                    {
                        "index": int(idx),
                        "timestamp": timestamp.isoformat(),
                        "row": {col: row.get(col) for col in cfg.data_columns},
                    }
                )

        issues["stats"]["missing_field_rows"] = len(issues["missing_rows"])
        issues["stats"]["invalid_rows"] = len(issues["invalid_rows"])

        timestamps = df[cfg.datetime_column].tolist()
        expected_delta = cfg.expected_timedelta()
        if expected_delta is not None and timestamps:
            for prev, current in zip(timestamps, timestamps[1:]):
                delta = current - prev
                if cfg.frequency == "daily":
                    if delta > pd.Timedelta(days=1):
                        missing_range = pd.date_range(prev + pd.Timedelta(days=1), current - pd.Timedelta(days=1), freq="D")
                        for missing in missing_range:
                            if cfg.allow_weekends or is_business_day(missing):
                                missing_dates.append(missing)
                else:
                    if expected_delta and delta > expected_delta:
                        steps = int(delta / expected_delta)
                        for step in range(1, steps):
                            missing_dates.append(prev + step * expected_delta)

        if timestamps:
            last_ts = timestamps[-1]
            now_ts = pd.Timestamp.utcnow().tz_localize(None)
            if cfg.frequency == "daily":
                expected_last = now_ts.normalize()
                while not cfg.allow_weekends and not is_business_day(expected_last):
                    expected_last -= pd.Timedelta(days=1)
                if last_ts.normalize() < expected_last:
                    future_range = pd.date_range(last_ts + pd.Timedelta(days=1), expected_last, freq="D")
                    for missing in future_range:
                        if cfg.allow_weekends or is_business_day(missing):
                            missing_dates.append(missing)
            elif expected_delta is not None:
                if now_ts - last_ts > expected_delta * 2:
                    steps = int(((now_ts - last_ts) / expected_delta))
                    for step in range(1, steps + 1):
                        missing_dates.append(last_ts + step * expected_delta)

        unique_missing = sorted({pd.Timestamp(ts) for ts in missing_dates})
        issues["missing_dates"] = [ts.isoformat() for ts in unique_missing]
        return issues

    def analyse_all_tables(self) -> Dict[str, Any]:
        results = {}
        for table_cfg in self.cfg.tables:
            results[table_cfg.table] = self.analyse_table(table_cfg)
        return results

    def refill_data(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        if not self.cfg.enable_refill:
            self.logger.info("Relleno deshabilitado por configuración")
            return {}
        refill_report: Dict[str, Any] = {}
        for table_cfg in self.cfg.tables:
            table_result = analysis.get(table_cfg.table, {})
            asset_reports = table_result.get("assets", {}) if isinstance(table_result, dict) else {}
            table_actions: List[Dict[str, Any]] = []
            for asset_id, asset_report in asset_reports.items():
                actions = self._refill_asset(table_cfg, asset_id, asset_report)
                if actions:
                    table_actions.extend(actions)
            if table_actions:
                refill_report[table_cfg.table] = table_actions
        return refill_report

    def _refill_asset(self, cfg: TimeseriesTableConfig, asset_id: Any, asset_report: Dict[str, Any]) -> List[Dict[str, Any]]:
        timestamps: List[pd.Timestamp] = []
        for date_str in asset_report.get("missing_dates", []):
            try:
                timestamps.append(pd.Timestamp(date_str))
            except Exception:
                continue
        for row_issue in asset_report.get("missing_rows", []) + asset_report.get("invalid_rows", []):
            ts = row_issue.get("timestamp")
            if ts:
                try:
                    timestamps.append(pd.Timestamp(ts))
                except Exception:
                    continue
        if not timestamps:
            return []

        metadata = dict(asset_report.get("metadata") or {})
        metadata.update(self.asset_metadata.get(asset_id, {}))
        symbol = metadata.get("symbol") or metadata.get("ticker") or metadata.get("symbol_local")
        epic = metadata.get("epic") or metadata.get("epic_code")
        actions: List[Dict[str, Any]] = []

        resolved_ts, derivation_actions, derived_fields = self._attempt_derivations(cfg, asset_id, asset_report)
        actions.extend(derivation_actions)
        if resolved_ts:
            timestamps = [
                ts
                for ts in timestamps
                if (ts.normalize() if cfg.frequency == "daily" else ts) not in resolved_ts
            ]

        timestamps = sorted({ts.normalize() if cfg.frequency == "daily" else ts for ts in timestamps})
        expected_delta = cfg.expected_timedelta() or pd.Timedelta(days=1)
        groups: List[List[pd.Timestamp]] = []
        current_group: List[pd.Timestamp] = []
        for ts in timestamps:
            if not current_group:
                current_group = [ts]
                continue
            if ts - current_group[-1] <= expected_delta * 2:
                current_group.append(ts)
            else:
                groups.append(current_group)
                current_group = [ts]
        if current_group:
            groups.append(current_group)

        for group in groups:
            group_ts = sorted(group)
            start = group_ts[0]
            end = group_ts[-1]
            skipped_flags: List[str] = []
            eligible_ts: List[pd.Timestamp] = []
            for ts in group_ts:
                dt_value = ts.to_pydatetime()
                flag = self.db.get_flag(cfg.table, asset_id, dt_value)
                if flag and flag.get("failure_count", 0) >= 3:
                    skipped_flags.append(ts.isoformat())
                    continue
                eligible_ts.append(ts)
            if not eligible_ts:
                actions.append(
                    {
                        "asset_id": asset_id,
                        "rango": (start.isoformat(), end.isoformat()),
                        "skipped_flags": skipped_flags,
                        "resultado": "omitido",
                    }
                )
                continue

            fields_needed = self._fields_needed(cfg, asset_report, eligible_ts, derived_fields)
            fetched, source_detail = self._fetch_range(
                cfg, symbol, epic, eligible_ts, start, end, fields_needed
            )
            normalized = self._normalize_records(cfg, fetched)
            filtered_records = [rec for rec in normalized if pd.Timestamp(rec["timestamp"]) in set(eligible_ts)]
            validated = [rec for rec in filtered_records if self._validate_row(cfg, rec)]
            if not validated:
                error_msg = "Sin datos válidos para el rango"
                for ts in eligible_ts:
                    self.db.upsert_flag(cfg.table, asset_id, ts.to_pydatetime(), error_msg)
                actions.append(
                    {
                        "asset_id": asset_id,
                        "rango": (start.isoformat(), end.isoformat()),
                        "error": error_msg,
                        "source": self._select_source(cfg),
                        "source_detail": source_detail,
                        "campos_solicitados": sorted(fields_needed),
                        "solicitudes": len(fetched),
                        "skipped_flags": skipped_flags,
                        "resultado": "sin_datos",
                    }
                )
                continue

            columns, rows = self._prepare_insert_rows(cfg, asset_id, validated)
            try:
                self.db.insert_rows(cfg.table, columns, rows, cfg.conflict_columns or [cfg.asset_column, cfg.datetime_column])
                for ts in eligible_ts:
                    self.db.clear_flag(cfg.table, asset_id, ts.to_pydatetime())
                actions.append(
                    {
                        "asset_id": asset_id,
                        "rango": (start.isoformat(), end.isoformat()),
                        "insertados": len(validated),
                        "source": self._select_source(cfg),
                        "source_detail": source_detail,
                        "campos_solicitados": sorted(fields_needed),
                        "skipped_flags": skipped_flags,
                        "resultado": "actualizado",
                    }
                )
            except Exception as exc:
                error_msg = str(exc)
                for ts in eligible_ts:
                    self.db.upsert_flag(cfg.table, asset_id, ts.to_pydatetime(), error_msg)
                actions.append(
                    {
                        "asset_id": asset_id,
                        "rango": (start.isoformat(), end.isoformat()),
                        "error": error_msg,
                        "source": self._select_source(cfg),
                        "source_detail": source_detail,
                        "campos_solicitados": sorted(fields_needed),
                        "skipped_flags": skipped_flags,
                        "resultado": "error",
                    }
                )
        return actions

    def _normalize_for_cfg(self, cfg: TimeseriesTableConfig, ts: pd.Timestamp) -> pd.Timestamp:
        return ts.normalize() if cfg.frequency == "daily" else ts

    def _attempt_derivations(
        self,
        cfg: TimeseriesTableConfig,
        asset_id: Any,
        asset_report: Dict[str, Any],
    ) -> Tuple[Set[pd.Timestamp], List[Dict[str, Any]], Dict[pd.Timestamp, Set[str]]]:
        resolved: Set[pd.Timestamp] = set()
        actions: List[Dict[str, Any]] = []
        derived_fields: Dict[pd.Timestamp, Set[str]] = {}
        processed: Set[pd.Timestamp] = set()

        issues = (asset_report.get("missing_rows", []) or []) + (asset_report.get("invalid_rows", []) or [])
        for issue in issues:
            ts_raw = issue.get("timestamp")
            if not ts_raw:
                continue
            try:
                ts = pd.Timestamp(ts_raw)
            except Exception:
                continue
            ts_key = self._normalize_for_cfg(cfg, ts)
            if ts_key in processed:
                continue
            processed.add(ts_key)
            dt_value = ts.to_pydatetime()
            row = self.db.fetch_row(cfg.table, cfg.asset_column, asset_id, cfg.datetime_column, dt_value)
            if not row:
                continue
            updates = self._derive_row_values(row)
            if updates:
                try:
                    updated = self.db.update_row(
                        cfg.table,
                        cfg.asset_column,
                        asset_id,
                        cfg.datetime_column,
                        dt_value,
                        updates,
                    )
                except Exception as exc:  # pragma: no cover - errores inesperados
                    actions.append(
                        {
                            "asset_id": asset_id,
                            "timestamp": ts.isoformat(),
                            "resultado": "error_derivacion",
                            "error": str(exc),
                            "campos": list(updates.keys()),
                            "source": "derivacion_local",
                            "source_detail": {"metodo": "derivacion_local"},
                        }
                    )
                    continue
                if updated:
                    actions.append(
                        {
                            "asset_id": asset_id,
                            "timestamp": ts.isoformat(),
                            "resultado": "derivado",
                            "metodo": "calculo_local",
                            "campos": {key: updates[key] for key in updates},
                            "source": "derivacion_local",
                            "source_detail": {"metodo": "derivacion_local"},
                        }
                    )
                    derived_fields.setdefault(ts_key, set()).update(updates.keys())
                    row.update(updates)
            if self._row_is_complete(cfg, row):
                resolved.add(ts_key)
        return resolved, actions, derived_fields

    def _derive_row_values(self, row: Dict[str, Any]) -> Dict[str, Any]:
        updates: Dict[str, Any] = {}

        def needs(field: str) -> bool:
            return field in row and is_missing_value(row.get(field))

        def has(field: str) -> bool:
            return field in row and not is_missing_value(row.get(field))

        changed = True
        while changed:
            changed = False

            if needs("divadj_open") and has("divadj_close") and has("open") and has("close"):
                close_px = as_float(row.get("close"))
                open_px = as_float(row.get("open"))
                div_close = as_float(row.get("divadj_close"))
                if close_px not in (None, 0) and open_px is not None and div_close is not None:
                    factor = div_close / close_px if close_px else None
                    if factor is not None:
                        value = open_px * factor
                        row["divadj_open"] = value
                        updates["divadj_open"] = value
                        changed = True

            if needs("divadj_high") and has("divadj_close") and has("high") and has("close"):
                close_px = as_float(row.get("close"))
                high_px = as_float(row.get("high"))
                div_close = as_float(row.get("divadj_close"))
                if close_px not in (None, 0) and high_px is not None and div_close is not None:
                    factor = div_close / close_px if close_px else None
                    if factor is not None:
                        value = high_px * factor
                        row["divadj_high"] = value
                        updates["divadj_high"] = value
                        changed = True

            if needs("divadj_low") and has("divadj_close") and has("low") and has("close"):
                close_px = as_float(row.get("close"))
                low_px = as_float(row.get("low"))
                div_close = as_float(row.get("divadj_close"))
                if close_px not in (None, 0) and low_px is not None and div_close is not None:
                    factor = div_close / close_px if close_px else None
                    if factor is not None:
                        value = low_px * factor
                        row["divadj_low"] = value
                        updates["divadj_low"] = value
                        changed = True

            if (needs("change") or needs("change_percent")) and has("open") and has("close"):
                open_px = as_float(row.get("open"))
                close_px = as_float(row.get("close"))
                if open_px is not None and close_px is not None:
                    change_val = close_px - open_px
                    if needs("change"):
                        row["change"] = change_val
                        updates["change"] = change_val
                        changed = True
                    if needs("change_percent") and open_px != 0:
                        change_pct = (change_val / open_px) * 100.0
                        row["change_percent"] = change_pct
                        updates["change_percent"] = change_pct
                        changed = True

            if needs("ts_epoch") and ("fecha" in row or "timestamp" in row):
                fecha_val = row.get("fecha") or row.get("timestamp")
                try:
                    ts_val = pd.Timestamp(fecha_val)
                except Exception:
                    ts_val = None
                if ts_val is not None:
                    epoch_val = int(ts_val.timestamp())
                    row["ts_epoch"] = epoch_val
                    updates["ts_epoch"] = epoch_val
                    changed = True

            if needs("spread_open") and has("open_bid") and has("open_ask"):
                bid = as_float(row.get("open_bid"))
                ask = as_float(row.get("open_ask"))
                if bid is not None and ask is not None:
                    spread = ask - bid
                    row["spread_open"] = spread
                    updates["spread_open"] = spread
                    changed = True

            if needs("spread_close") and has("close_bid") and has("close_ask"):
                bid = as_float(row.get("close_bid"))
                ask = as_float(row.get("close_ask"))
                if bid is not None and ask is not None:
                    spread = ask - bid
                    row["spread_close"] = spread
                    updates["spread_close"] = spread
                    changed = True

            if needs("open_bid") and has("open") and has("spread_open"):
                open_px = as_float(row.get("open"))
                spread = as_float(row.get("spread_open"))
                if open_px is not None and spread is not None:
                    bid = open_px - spread / 2.0
                    row["open_bid"] = bid
                    updates["open_bid"] = bid
                    changed = True

            if needs("close_bid") and has("close") and has("spread_close"):
                close_px = as_float(row.get("close"))
                spread = as_float(row.get("spread_close"))
                if close_px is not None and spread is not None:
                    bid = close_px - spread / 2.0
                    row["close_bid"] = bid
                    updates["close_bid"] = bid
                    changed = True

            if needs("open_ask") and has("open") and has("spread_open"):
                open_px = as_float(row.get("open"))
                spread = as_float(row.get("spread_open"))
                if open_px is not None and spread is not None:
                    ask = open_px + spread / 2.0
                    row["open_ask"] = ask
                    updates["open_ask"] = ask
                    changed = True

            if needs("open") and has("open_bid") and has("open_ask"):
                bid = as_float(row.get("open_bid"))
                ask = as_float(row.get("open_ask"))
                if bid is not None and ask is not None:
                    open_px = (bid + ask) / 2.0
                    row["open"] = open_px
                    updates["open"] = open_px
                    changed = True

            if needs("close_ask") and has("close") and has("spread_close"):
                close_px = as_float(row.get("close"))
                spread = as_float(row.get("spread_close"))
                if close_px is not None and spread is not None:
                    ask = close_px + spread / 2.0
                    row["close_ask"] = ask
                    updates["close_ask"] = ask
                    changed = True

            if needs("close") and has("close_bid") and has("close_ask"):
                bid = as_float(row.get("close_bid"))
                ask = as_float(row.get("close_ask"))
                if bid is not None and ask is not None:
                    close_px = (bid + ask) / 2.0
                    row["close"] = close_px
                    updates["close"] = close_px
                    changed = True

        return updates

    def _row_is_complete(self, cfg: TimeseriesTableConfig, row: Dict[str, Any]) -> bool:
        for column in cfg.data_columns:
            if column not in row:
                continue
            value = row.get(column)
            if is_missing_value(value):
                return False
            if column in NUMERIC_COLUMNS:
                numeric_val = as_float(value)
                if numeric_val is None:
                    return False
                if column == "volume" and numeric_val < 0:
                    return False
            if column in INTEGER_COLUMNS:
                val = row.get(column)
                if val is None:
                    return False
                if isinstance(val, float) and math.isnan(val):
                    return False
                if isinstance(val, Decimal):
                    try:
                        float(val)
                    except (TypeError, ValueError):
                        return False
                if isinstance(val, float):
                    continue
                if isinstance(val, Decimal):
                    continue
                if not isinstance(val, int):
                    return False

        open_px = as_float(row.get("open")) if "open" in row else None
        high_px = as_float(row.get("high")) if "high" in row else None
        low_px = as_float(row.get("low")) if "low" in row else None
        close_px = as_float(row.get("close")) if "close" in row else None

        if open_px is not None and high_px is not None and open_px > high_px + 1e-9:
            return False
        if close_px is not None and high_px is not None and close_px > high_px + 1e-9:
            return False
        if low_px is not None and open_px is not None and low_px > open_px + 1e-9:
            return False
        if low_px is not None and close_px is not None and low_px > close_px + 1e-9:
            return False
        if high_px is not None and low_px is not None and high_px < low_px:
            return False
        if "volume" in row:
            volume_val = as_float(row.get("volume"))
            if volume_val is not None and volume_val < 0:
                return False
        return True

    def _fields_needed(
        self,
        cfg: TimeseriesTableConfig,
        asset_report: Dict[str, Any],
        timestamps: List[pd.Timestamp],
        derived_fields: Dict[pd.Timestamp, Set[str]],
    ) -> Set[str]:
        needed: Set[str] = set()
        ts_set = {self._normalize_for_cfg(cfg, ts) for ts in timestamps}

        for row_issue in asset_report.get("missing_rows", []) or []:
            ts_raw = row_issue.get("timestamp")
            if not ts_raw:
                continue
            try:
                ts = pd.Timestamp(ts_raw)
            except Exception:
                continue
            ts_key = self._normalize_for_cfg(cfg, ts)
            if ts_key not in ts_set:
                continue
            row_data = row_issue.get("row") or {}
            for column in cfg.data_columns:
                if column not in row_data:
                    continue
                if column in derived_fields.get(ts_key, set()):
                    continue
                value = row_data.get(column)
                if is_missing_value(value) or (column in NUMERIC_COLUMNS and as_float(value) is None):
                    needed.add(column)

        for row_issue in asset_report.get("invalid_rows", []) or []:
            ts_raw = row_issue.get("timestamp")
            if not ts_raw:
                continue
            try:
                ts = pd.Timestamp(ts_raw)
            except Exception:
                continue
            ts_key = self._normalize_for_cfg(cfg, ts)
            if ts_key in ts_set:
                needed.update(cfg.data_columns)

        for ts_raw in asset_report.get("missing_dates", []) or []:
            try:
                ts = pd.Timestamp(ts_raw)
            except Exception:
                continue
            ts_key = self._normalize_for_cfg(cfg, ts)
            if ts_key in ts_set:
                needed.update(cfg.data_columns)

        if not needed:
            needed.update(cfg.data_columns)
        return needed

    def _build_source_detail(
        self,
        cfg: TimeseriesTableConfig,
        fields_needed: Set[str],
        interval_minutes: Optional[int] = None,
        fallback_used: bool = False,
    ) -> Dict[str, Any]:
        source = self._select_source(cfg)
        fields_sorted = sorted(fields_needed)
        if source == "capital":
            mapping = {field: CAPITAL_FIELD_MAP.get(field, field) for field in fields_sorted}
            return {
                "provider": "Capital.com",
                "endpoint": "/prices/{epic}",
                "requested_fields": fields_sorted,
                "field_map": mapping,
            }

        if source == "fmp":
            if cfg.frequency == "daily":
                mapping = {field: FMP_DAILY_FIELD_MAP.get(field, field) for field in fields_sorted}
                detail: Dict[str, Any] = {
                    "provider": "Financial Modeling Prep",
                    "endpoint": "/historical-price-full/{symbol}",
                    "requested_fields": fields_sorted,
                    "field_map": mapping,
                }
            else:
                mapping = {field: FMP_INTRADAY_FIELD_MAP.get(field, field) for field in fields_sorted}
                interval = interval_minutes or cfg.interval_minutes or 1
                detail = {
                    "provider": "Financial Modeling Prep",
                    "endpoint": f"/historical-chart/{interval}min/{{symbol}}",
                    "requested_fields": fields_sorted,
                    "field_map": mapping,
                }
            if fallback_used and HAS_YFINANCE:
                detail["fallback"] = {
                    "provider": "Yahoo Finance",
                    "endpoint": "yfinance.download",
                    "field_map": {field: YF_FIELD_MAP.get(field, field) for field in fields_sorted},
                }
            return detail

        return {
            "provider": source,
            "requested_fields": fields_sorted,
            "field_map": {field: field for field in fields_sorted},
        }

    def _select_source(self, cfg: TimeseriesTableConfig) -> str:
        if cfg.table.endswith("_cfd"):
            return "capital"
        return "fmp"

    def _fetch_range(
        self,
        cfg: TimeseriesTableConfig,
        symbol: Optional[str],
        epic: Optional[str],
        timestamps: List[pd.Timestamp],
        start: pd.Timestamp,
        end: pd.Timestamp,
        fields_needed: Set[str],
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        source = self._select_source(cfg)
        if source == "capital":
            if not epic:
                self.logger.warning("No se dispone de epic para %s en %s", symbol, cfg.table)
                return [], self._build_source_detail(cfg, fields_needed)
            resolution = "DAY" if cfg.frequency == "daily" else f"MINUTE_{cfg.interval_minutes or 1}"
            records = self.capital.fetch_prices(epic, start.to_pydatetime(), end.to_pydatetime(), resolution)
            return records, self._build_source_detail(cfg, fields_needed)
        if source == "fmp":
            if not symbol:
                self.logger.warning("No se dispone de símbolo para activo %s en %s", timestamps, cfg.table)
                return [], self._build_source_detail(cfg, fields_needed)
            if cfg.frequency == "daily":
                records = self.fmp.fetch_daily(symbol, start.date(), end.date())
                fallback_used = False
            else:
                records = self.fmp.fetch_intraday(symbol, cfg.interval_minutes or 1, start.to_pydatetime(), end.to_pydatetime())
                fallback_used = False
            if not records and HAS_YFINANCE:
                if cfg.frequency == "daily":
                    yf_records = self.yf_client.fetch(symbol, start.to_pydatetime(), end.to_pydatetime(), interval="1d")
                else:
                    interval = "1m" if (cfg.interval_minutes or 1) <= 1 else f"{cfg.interval_minutes}m"
                    yf_records = self.yf_client.fetch(symbol, start.to_pydatetime(), end.to_pydatetime(), interval=interval)
                if yf_records:
                    records.extend(yf_records)
                    fallback_used = True
            detail = self._build_source_detail(
                cfg,
                fields_needed,
                interval_minutes=cfg.interval_minutes or 1,
                fallback_used=fallback_used,
            )
            return records, detail
        return [], self._build_source_detail(cfg, fields_needed)

    def _normalize_records(self, cfg: TimeseriesTableConfig, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for rec in records:
            norm = self._normalize_record(cfg, rec)
            if norm:
                normalized.append(norm)
        return normalized

    def _normalize_record(self, cfg: TimeseriesTableConfig, rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if "snapshotTimeUTC" in rec or "snapshotTime" in rec:
            timestamp = rec.get("snapshotTimeUTC") or rec.get("snapshotTime")
            timestamp = pd.Timestamp(timestamp).to_pydatetime()
            def pick_price(node: Any) -> Optional[float]:
                if isinstance(node, (int, float)):
                    return float(node)
                if isinstance(node, dict):
                    for key in ("mid", "lastTraded", "value", "bid", "ask"):
                        val = node.get(key)
                        if val is not None:
                            try:
                                return float(val)
                            except (TypeError, ValueError):
                                continue
                return None
            norm = {
                "timestamp": timestamp,
                "open": pick_price(rec.get("openPrice")),
                "high": pick_price(rec.get("highPrice")),
                "low": pick_price(rec.get("lowPrice")),
                "close": pick_price(rec.get("closePrice")),
                "volume": as_float(rec.get("lastTradedVolume")),
            }
            return norm
        if "date" in rec:
            timestamp = pd.Timestamp(rec.get("date")).to_pydatetime()
            norm = {
                "timestamp": timestamp,
                "open": as_float(rec.get("open")),
                "high": as_float(rec.get("high")),
                "low": as_float(rec.get("low")),
                "close": as_float(rec.get("close")),
                "volume": as_float(rec.get("volume")),
            }
            return norm
        if "timestamp" in rec:
            timestamp = pd.Timestamp(rec.get("timestamp")).to_pydatetime()
            norm = {
                "timestamp": timestamp,
                "open": as_float(rec.get("open")),
                "high": as_float(rec.get("high")),
                "low": as_float(rec.get("low")),
                "close": as_float(rec.get("close")),
                "volume": as_float(rec.get("volume")),
            }
            return norm
        return None

    def _prepare_insert_rows(
        self, cfg: TimeseriesTableConfig, asset_id: Any, records: List[Dict[str, Any]]
    ) -> Tuple[Sequence[str], List[Sequence[Any]]]:
        columns = [cfg.asset_column, cfg.datetime_column] + cfg.data_columns
        rows: List[Sequence[Any]] = []
        for rec in records:
            timestamp = pd.Timestamp(rec["timestamp"]).to_pydatetime()
            row = [asset_id, timestamp]
            for col in cfg.data_columns:
                row.append(rec.get(col))
            rows.append(row)
        return columns, rows

    def _validate_row(self, cfg: TimeseriesTableConfig, rec: Dict[str, Any]) -> bool:
        open_px = rec.get("open")
        high_px = rec.get("high")
        low_px = rec.get("low")
        close_px = rec.get("close")
        volume = rec.get("volume")
        for key, value in (("open", open_px), ("high", high_px), ("low", low_px), ("close", close_px)):
            if value is None:
                return False
            if not isinstance(value, (int, float)):
                return False
            if value < 0:
                return False
            if cfg.price_floor is not None and value < cfg.price_floor:
                return False
            if cfg.price_ceiling is not None and value > cfg.price_ceiling:
                return False
        if volume is None or (isinstance(volume, (int, float)) and volume < 0):
            return False
        if open_px is not None and high_px is not None and open_px > high_px + 1e-9:
            return False
        if close_px is not None and high_px is not None and close_px > high_px + 1e-9:
            return False
        if low_px is not None and open_px is not None and low_px > open_px + 1e-9:
            return False
        if low_px is not None and close_px is not None and low_px > close_px + 1e-9:
            return False
        if high_px is not None and low_px is not None and high_px < low_px:
            return False
        return True

    def compute_gap_stats(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        totals = {
            "rows": 0,
            "issues": 0,
        }
        per_table: Dict[str, Any] = {}
        for table_cfg in self.cfg.tables:
            table_data = analysis.get(table_cfg.table, {})
            assets = table_data.get("assets", {}) if isinstance(table_data, dict) else {}
            table_rows = 0
            table_issues = 0
            asset_stats: Dict[str, Any] = {}
            for asset_id, asset_report in assets.items():
                stats = asset_report.get("stats", {})
                rows = stats.get("rows", 0)
                missing_rows = stats.get("missing_field_rows", 0)
                invalid_rows = stats.get("invalid_rows", 0)
                missing_dates = len(asset_report.get("missing_dates", []))
                total_rows = rows + missing_dates
                issue_rows = missing_rows + invalid_rows + missing_dates
                pct = float(issue_rows) / total_rows * 100 if total_rows else 0.0
                asset_stats[asset_id] = {
                    "rows": rows,
                    "missing_dates": missing_dates,
                    "issue_rows": issue_rows,
                    "issue_pct": pct,
                }
                table_rows += total_rows
                table_issues += issue_rows
            table_pct = float(table_issues) / table_rows * 100 if table_rows else 0.0
            per_table[table_cfg.table] = {
                "rows": table_rows,
                "issues": table_issues,
                "issue_pct": table_pct,
                "assets": asset_stats,
                "sin_asset_id": table_data.get("sin_asset_id"),
            }
            totals["rows"] += table_rows
            totals["issues"] += table_issues
        totals["issue_pct"] = float(totals["issues"]) / totals["rows"] * 100 if totals["rows"] else 0.0
        return {
            "totales": totals,
            "tablas": per_table,
        }

    def _config_payload(self) -> Dict[str, Any]:
        payload = asdict(self.cfg)
        payload["reports_dir"] = str(self.cfg.reports_dir)
        payload["logs_dir"] = str(self.cfg.logs_dir)
        tables_payload = []
        for table_cfg in self.cfg.tables:
            tbl = asdict(table_cfg)
            tbl["conflict_columns"] = list(table_cfg.conflict_columns)
            tables_payload.append(tbl)
        payload["tables"] = tables_payload
        return payload

    def config_hash(self) -> str:
        payload = self._config_payload()
        return sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()

    def save_audit_log(
        self,
        schema_report: Dict[str, Any],
        analysis_before: Dict[str, Any],
        refill_report: Dict[str, Any],
        analysis_after: Optional[Dict[str, Any]],
        stats_before: Dict[str, Any],
        stats_after: Optional[Dict[str, Any]],
        flags: List[Dict[str, Any]],
    ) -> None:
        cfg_hash = self.config_hash()
        payload = {
            "config": self._config_payload(),
            "schema": schema_report,
            "analisis_inicial": analysis_before,
            "relleno": refill_report,
            "analisis_final": analysis_after,
            "estadisticas_iniciales": stats_before,
            "estadisticas_finales": stats_after,
            "timestamp": dt.datetime.utcnow().isoformat(),
            "flags": flags,
        }
        self.db.ensure_tables(self.cfg.audit_table, self.cfg.flags_table)
        query = f"INSERT INTO {self.cfg.audit_table} (config_hash, report) VALUES (%s, %s)"
        self.db.execute(query, (cfg_hash, json.dumps(payload, default=str)))

    def export_report(self, payload: Dict[str, Any]) -> Path:
        self.cfg.reports_dir.mkdir(parents=True, exist_ok=True)
        filename = self.cfg.reports_dir / f"auditoria_completitud_{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, default=str)
        return filename

class NullCapitalComClient:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger.getChild("CapitalComNull")

    def fetch_prices(self, epic: str, start: dt.datetime, end: dt.datetime, resolution: str) -> List[Dict[str, Any]]:
        self.logger.warning(
            "Cliente Capital.com no disponible. Se omite petición de %s (%s-%s) resolución %s",
            epic,
            start,
            end,
            resolution,
        )
        return []

def print_summary(
    schema_report: Dict[str, Any],
    stats_before: Dict[str, Any],
    stats_after: Optional[Dict[str, Any]],
    refill_report: Dict[str, Any],
    flags: List[Dict[str, Any]],
) -> None:
    print("\n===== Resumen de Auditoría de Completitud =====")
    orphans = schema_report.get("elementos_huerfanos", [])
    print("Elementos huérfanos detectados:")
    if not orphans:
        print("  - Ninguno")
    else:
        for item in orphans:
            print(
                f"  - Tabla {item.get('tabla')} columna {item.get('columna')} sin referencia en {item.get('padre')} ({item.get('registros_huerfanos')} registros)"
            )

    print("\nPorcentaje de huecos iniciales:")
    for table, info in stats_before.get("tablas", {}).items():
        print(f"  - {table}: {info.get('issue_pct', 0):.2f}% de huecos (rows={info.get('rows')}, issues={info.get('issues')})")
        for asset_id, asset_stats in info.get("assets", {}).items():
            print(
                f"      · Activo {asset_id}: {asset_stats.get('issue_pct', 0):.2f}% ({asset_stats.get('issue_rows')} huecos sobre {asset_stats.get('rows')} filas + {asset_stats.get('missing_dates')} fechas)"
            )

    if stats_after:
        print("\nPorcentaje de huecos tras el relleno:")
        for table, info in stats_after.get("tablas", {}).items():
            print(f"  - {table}: {info.get('issue_pct', 0):.2f}% de huecos")
            for asset_id, asset_stats in info.get("assets", {}).items():
                print(
                    f"      · Activo {asset_id}: {asset_stats.get('issue_pct', 0):.2f}% ({asset_stats.get('issue_rows')} huecos)"
                )
        print(
            f"Total base de datos: {stats_after['totales'].get('issue_pct', 0):.2f}% (antes {stats_before['totales'].get('issue_pct', 0):.2f}%)"
        )
    else:
        print(
            f"Total base de datos: {stats_before['totales'].get('issue_pct', 0):.2f}% (re-análisis deshabilitado)"
        )

    print("\nAcciones de relleno ejecutadas:")
    if not refill_report:
        print("  - No se completaron huecos o relleno deshabilitado.")
    else:
        for table, actions in refill_report.items():
            print(f"  - Tabla {table}:")
            for action in actions:
                status = action.get("resultado")
                rng = action.get("rango")
                source = action.get("source", "-")
                if status == "actualizado":
                    print(
                        f"      · Activo {action.get('asset_id')} {rng} rellenado ({action.get('insertados')} registros) desde {source}"
                    )
                elif status == "derivado":
                    campos = action.get("campos") or {}
                    if isinstance(campos, dict):
                        campos_list = ", ".join(sorted(campos.keys()))
                    elif isinstance(campos, (list, set, tuple)):
                        campos_list = ", ".join(sorted(campos))
                    else:
                        campos_list = str(campos)
                    ts = action.get("timestamp", rng if isinstance(rng, str) else "-")
                    print(
                        f"      · Activo {action.get('asset_id')} fecha {ts} completado por derivación local ({campos_list})"
                    )
                elif status == "omitido":
                    print(
                        f"      · Activo {action.get('asset_id')} {rng} omitido por flags previos ({len(action.get('skipped_flags', []))} fechas)"
                    )
                else:
                    print(
                        f"      · Activo {action.get('asset_id')} {rng} fallo ({status}) fuente {source}: {action.get('error')}"
                    )

    print("\nFlags activos (intentos fallidos >= 3):")
    if not flags:
        print("  - Ninguno")
    else:
        for flag in flags:
            print(
                f"  - Tabla {flag.get('table_name')} activo {flag.get('asset_id')} fecha {flag.get('datetime_value')} (intentos={flag.get('failure_count')})"
            )
    print("===== Fin del resumen =====\n")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auditoría y completitud de series financieras")
    parser.add_argument("--max-activos", type=int, dest="max_activos", help="Limita el número de activos a analizar por tabla")
    parser.add_argument("--sin-rellenar", action="store_true", help="No intenta completar huecos con APIs externas")
    parser.add_argument("--sin-reanalizar", action="store_true", help="No realiza un segundo análisis tras el relleno")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    cfg = AppConfig.from_env()
    if args.max_activos is not None:
        cfg.max_assets = args.max_activos
    if args.sin_rellenar:
        cfg.enable_refill = False
    if args.sin_reanalizar:
        cfg.reanalyse_after_fill = False

    setup_logging(cfg)
    logger = logging.getLogger("completitud")

    try:
        with DatabaseManager(cfg, logger) as db:
            try:
                capital_client = CapitalComClient(cfg, logger)
            except Exception as exc:
                logger.error("No se pudo iniciar cliente Capital.com: %s", exc)
                capital_client = NullCapitalComClient(logger)
            fmp_client = FMPClient(cfg, logger)
            yf_client = YahooFinanceClient(logger)
            auditor = DataCompletenessAuditor(cfg, db, capital_client, fmp_client, yf_client)

            schema_report = auditor.inspect_schema()
            analysis_before = auditor.analyse_all_tables()
            stats_before = auditor.compute_gap_stats(analysis_before)
            refill_report = auditor.refill_data(analysis_before)

            analysis_after: Optional[Dict[str, Any]] = None
            stats_after: Optional[Dict[str, Any]] = None
            if cfg.reanalyse_after_fill:
                analysis_after = auditor.analyse_all_tables()
                stats_after = auditor.compute_gap_stats(analysis_after)

            flags = db.list_flags()

            report_payload = {
                "schema": schema_report,
                "analisis_inicial": analysis_before,
                "relleno": refill_report,
                "analisis_final": analysis_after,
                "estadisticas_iniciales": stats_before,
                "estadisticas_finales": stats_after,
                "flags": flags,
            }
            report_path = auditor.export_report(report_payload)
            auditor.save_audit_log(
                schema_report,
                analysis_before,
                refill_report,
                analysis_after,
                stats_before,
                stats_after,
                flags,
            )
            logger.info("Informe detallado almacenado en %s", report_path)
            print_summary(schema_report, stats_before, stats_after, refill_report, flags)
    except Exception as exc:
        logger.exception("Ejecución abortada por error crítico: %s", exc)
        raise


if __name__ == "__main__":
    main()
