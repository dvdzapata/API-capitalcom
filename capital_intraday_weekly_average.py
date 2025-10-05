"""Genera promedios intradía por día de la semana para el índice US100.

El script se conecta a una base de datos PostgreSQL (credenciales en `.env`) y
extrae cotizaciones minuto a minuto de la tabla `cotizaciones_intradia_cfd`
filtrando por `symbol=US100` y `asset_id=97`. Cuando la configuración de
PostgreSQL no está disponible, puede seguir recurriendo a la API de Capital.com
como respaldo. Para cada día de la semana obtiene las últimas 50 sesiones
(00:00 a 00:00 UTC del día siguiente), calcula la media minuto a minuto y
genera un gráfico con el promedio intradía de cada día.

Por defecto sólo se procesan los días hábiles (lunes a viernes). Esto puede
ajustarse mediante la variable de entorno `CAPITAL_TRADING_WEEKDAYS` con una
lista de índices de día separados por comas (0=lunes ... 6=domingo).

Requisitos:
    pip install pandas requests matplotlib python-dotenv (opcional)
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence

import pandas as pd
import requests

try:  # Conexión a PostgreSQL
    import psycopg
    from psycopg import sql
    from psycopg.rows import dict_row
except ModuleNotFoundError:  # pragma: no cover - dependencia opcional
    psycopg = None  # type: ignore
    sql = None  # type: ignore
    dict_row = None  # type: ignore

try:  # Carga opcional de .env
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:  # pragma: no cover - dependencia opcional
    def load_dotenv(path: str | Path = ".env") -> None:
        """Carga simple de variables de entorno desde un archivo .env."""

        path = Path(path)
        if not path.exists():
            return
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)

    load_dotenv()


@dataclass
class CapitalCredentials:
    api_key: str
    email: str
    password: str


class CapitalAPIError(RuntimeError):
    """Errores de la API de Capital.com."""


class CapitalDateRangeTooLarge(CapitalAPIError):
    """Se solicitó un rango temporal superior al permitido por la API."""


@dataclass
class PostgresIntradayConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    symbol: str
    asset_id: int
    schema: str = "public"
    table: str = "cotizaciones_intradia_cfd"
    symbol_column: str = "symbol"
    asset_id_column: str = "asset_id"
    timestamp_column: str = "timestamp"
    price_column: Optional[str] = None
    price_candidates: List[str] = field(
        default_factory=lambda: ["close", "mid_price", "price", "last", "last_price"]
    )
    connect_timeout: int = 10
    oversample_multiplier: int = 3


class CapitalClient:
    BASE_URL = "https://api-capital.backend-capital.com/api/v1"

    def __init__(
        self,
        credentials: CapitalCredentials,
        logger: logging.Logger,
        request_timeout: int = 30,
        max_requests_per_minute: int = 50,
        cache_dir: Path | None = None,
        page_size: int = 1000,
    ) -> None:
        self.credentials = credentials
        self.logger = logger
        self.request_timeout = request_timeout
        self.max_requests_per_minute = max(request_timeout and 1, 1)
        self.max_requests_per_minute = max_requests_per_minute
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-CAP-API-KEY": credentials.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        self.cst: Optional[str] = None
        self.xst: Optional[str] = None
        self._last_request_ts: float = 0.0
        self.cache_dir = cache_dir
        if self.cache_dir:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        if page_size <= 0:
            raise ValueError("page_size debe ser positivo")
        self.page_size = min(page_size, 1000)

    def authenticate(self) -> None:
        payload = {"identifier": self.credentials.email, "password": self.credentials.password}
        self.logger.info("Autenticando con Capital.com")
        resp = self.session.post(
            f"{self.BASE_URL}/session",
            data=json.dumps(payload),
            timeout=self.request_timeout,
        )
        if resp.status_code not in (200, 201):
            raise CapitalAPIError(f"Login falló ({resp.status_code}): {resp.text}")

        self.cst = resp.headers.get("CST")
        self.xst = resp.headers.get("X-SECURITY-TOKEN")
        if not self.cst or not self.xst:
            body = {}
            try:
                body = resp.json()
            except ValueError:  # pragma: no cover - respuesta no JSON
                body = {}
            self.cst = self.cst or body.get("CST")
            self.xst = self.xst or body.get("securityToken") or body.get("X-SECURITY-TOKEN")
        if not self.cst or not self.xst:
            raise CapitalAPIError("No se recibieron tokens de seguridad")

        self.session.headers.update({"CST": self.cst, "X-SECURITY-TOKEN": self.xst})
        self.logger.info("Sesión establecida correctamente")

    def _respect_rate_limit(self) -> None:
        if self.max_requests_per_minute <= 0:
            return
        min_interval = 60.0 / self.max_requests_per_minute
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < min_interval:
            wait_for = min_interval - elapsed
            self.logger.debug("Pausando %.2fs para respetar el rate limit", wait_for)
            time.sleep(wait_for)

    def _cached_response_path(self, key: str) -> Optional[Path]:
        if not self.cache_dir:
            return None
        return self.cache_dir / f"{key}.json"

    def _read_cache(self, key: str) -> Optional[Dict[str, object]]:
        path = self._cached_response_path(key)
        if not path or not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            self.logger.debug("Respuesta reutilizada de caché %s", path)
            return data
        except json.JSONDecodeError:
            self.logger.warning("Caché corrupta en %s, se ignora", path)
            return None

    def _write_cache(self, key: str, data: Dict[str, object]) -> None:
        path = self._cached_response_path(key)
        if not path:
            return
        tmp_path = path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(data), encoding="utf-8")
        tmp_path.replace(path)

    def search_epic(self, search_term: str) -> List[Dict[str, object]]:
        key = f"search_{search_term}"
        cached = self._read_cache(key)
        if cached:
            return cached.get("markets", []) if isinstance(cached, dict) else []

        self._respect_rate_limit()
        resp = self.session.get(
            f"{self.BASE_URL}/markets",
            params={"searchTerm": search_term},
            timeout=self.request_timeout,
        )
        self._last_request_ts = time.monotonic()
        if resp.status_code != 200:
            raise CapitalAPIError(f"Búsqueda de {search_term} falló: {resp.status_code} {resp.text}")
        data = resp.json()
        markets = []
        for key_name in ("markets", "marketDetails"):
            entries = data.get(key_name)
            if isinstance(entries, dict):
                entries = [entries]
            if isinstance(entries, list):
                markets.extend([item for item in entries if isinstance(item, dict)])
        if not markets:
            self.logger.warning("Sin resultados de búsqueda para %s", search_term)
        self._write_cache(key, {"markets": markets})
        return markets

    def _min_span_for_resolution(self, resolution: str) -> timedelta:
        if resolution.startswith("MINUTE"):
            return timedelta(minutes=1)
        if resolution.startswith("HOUR"):
            return timedelta(hours=1)
        return timedelta(minutes=1)

    def _fetch_prices_chunk(
        self,
        epic: str,
        start: datetime,
        end: datetime,
        resolution: str,
    ) -> List[Dict[str, object]]:
        start_utc = start.astimezone(UTC) if start.tzinfo else start.replace(tzinfo=UTC)
        end_utc = end.astimezone(UTC) if end.tzinfo else end.replace(tzinfo=UTC)
        key = f"prices_{epic}_{start_utc:%Y%m%dT%H%M%S}_{end_utc:%Y%m%dT%H%M%S}_{resolution}"
        cached = self._read_cache(key)
        if cached:
            prices = cached.get("prices") if isinstance(cached, dict) else None
            if isinstance(prices, list):
                return [item for item in prices if isinstance(item, dict)]

        collected: List[Dict[str, object]] = []
        for attempt in range(1, 4):
            try:
                page_number = 1
                collected.clear()
                while True:
                    params = {
                        "resolution": resolution,
                        "from": start_utc.strftime("%Y-%m-%dT%H:%M:%S"),
                        "to": end_utc.strftime("%Y-%m-%dT%H:%M:%S"),
                        "pageSize": self.page_size,
                        "pageNumber": page_number,
                    }
                    self._respect_rate_limit()
                    resp = self.session.get(
                        f"{self.BASE_URL}/prices/{epic}",
                        params=params,
                        timeout=self.request_timeout,
                    )
                    self._last_request_ts = time.monotonic()
                    if resp.status_code != 200:
                        error_payload: Dict[str, object] = {}
                        try:
                            error_payload = resp.json()
                        except ValueError:  # pragma: no cover - respuesta no JSON
                            error_payload = {}
                        error_code = str(error_payload.get("errorCode", "")).lower()
                        if resp.status_code == 400 and error_code == "error.invalid.max.daterange":
                            raise CapitalDateRangeTooLarge(
                                f"Rango {start.isoformat()} - {end.isoformat()} demasiado amplio"
                            )
                        raise CapitalAPIError(
                            f"Capital.com devolvió {resp.status_code}: {resp.text[:200]}"
                        )
                    payload = resp.json()
                    prices = payload.get("prices", []) if isinstance(payload, dict) else []
                    page_records = (
                        [item for item in prices if isinstance(item, dict)]
                        if isinstance(prices, list)
                        else []
                    )
                    collected.extend(page_records)
                    self.logger.debug(
                        "Descargada página %s (%s registros) para %s %s-%s",
                        page_number,
                        len(page_records),
                        epic,
                        start_utc.isoformat(),
                        end_utc.isoformat(),
                    )
                    if len(page_records) < self.page_size:
                        break
                    page_number += 1
                self._write_cache(key, {"prices": collected})
                return list(collected)
            except CapitalDateRangeTooLarge:
                raise
            except Exception as exc:  # pragma: no cover - reintentos
                self.logger.warning("Intento %s falló al pedir precios: %s", attempt, exc)
                time.sleep(2 * attempt)

        raise CapitalAPIError(
            f"Falló la descarga de precios tras múltiples intentos ({start.isoformat()} - {end.isoformat()})"
        )

    def fetch_prices(
        self,
        epic: str,
        start: datetime,
        end: datetime,
        resolution: str = "MINUTE",
    ) -> List[Dict[str, object]]:
        if not self.cst or not self.xst:
            raise CapitalAPIError("Cliente no autenticado")

        pending: List[tuple[datetime, datetime]] = [(start, end)]
        collected: List[Dict[str, object]] = []
        min_span = self._min_span_for_resolution(resolution)

        while pending:
            chunk_start, chunk_end = pending.pop(0)
            if chunk_start >= chunk_end:
                continue
            try:
                chunk_records = self._fetch_prices_chunk(
                    epic=epic,
                    start=chunk_start,
                    end=chunk_end,
                    resolution=resolution,
                )
                collected.extend(chunk_records)
            except CapitalDateRangeTooLarge:
                duration = chunk_end - chunk_start
                if duration <= min_span:
                    self.logger.error(
                        "Rango mínimo alcanzado sin datos para %s entre %s y %s",
                        epic,
                        chunk_start.isoformat(),
                        chunk_end.isoformat(),
                    )
                    continue
                midpoint = chunk_start + duration / 2
                if midpoint <= chunk_start or midpoint >= chunk_end:
                    midpoint = chunk_start + min_span
                self.logger.debug(
                    "Dividiendo petición %s en %s - %s y %s - %s",
                    epic,
                    chunk_start.isoformat(),
                    midpoint.isoformat(),
                    midpoint.isoformat(),
                    chunk_end.isoformat(),
                )
                pending.insert(0, (midpoint, chunk_end))
                pending.insert(0, (chunk_start, midpoint))
                continue

        if not collected:
            self.logger.warning(
                "Sin datos devueltos para %s entre %s y %s",
                epic,
                start.isoformat(),
                end.isoformat(),
            )
            return []

        return collected


class PostgresIntradaySource:
    """Cargador de cotizaciones intradía desde PostgreSQL."""

    def __init__(self, config: PostgresIntradayConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger
        self._conn: Optional["psycopg.Connection"] = None
        self._available_columns: Optional[set[str]] = None
        self._resolved_price_column: Optional[str] = config.price_column

    def __enter__(self) -> "PostgresIntradaySource":
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[object],
    ) -> None:
        self.close()

    def connect(self) -> None:
        if psycopg is None or sql is None:
            raise RuntimeError(
                "psycopg no está instalado; ejecute `pip install psycopg[binary]` para habilitar PostgreSQL"
            )
        if self._conn is not None:
            return
        self._conn = psycopg.connect(  # type: ignore[call-arg]
            host=self.config.host,
            port=self.config.port,
            dbname=self.config.database,
            user=self.config.user,
            password=self.config.password,
            connect_timeout=self.config.connect_timeout,
        )
        self._conn.autocommit = False
        with self._conn.cursor() as cur:
            cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
            cur.execute("SET TIME ZONE 'UTC'")
        self.logger.info(
            "Conexión PostgreSQL establecida en %s:%s/%s (tabla %s)",
            self.config.host,
            self.config.port,
            self.config.database,
            f"{self.config.schema}.{self.config.table}" if self.config.schema else self.config.table,
        )
        self._load_available_columns()

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def _ensure_connection(self) -> "psycopg.Connection":
        if self._conn is None:
            self.connect()
        assert self._conn is not None
        return self._conn

    def _table_identifier(self) -> "sql.Composed":
        assert sql is not None
        if self.config.schema:
            return sql.Identifier(self.config.schema, self.config.table)
        return sql.Identifier(self.config.table)

    def _load_available_columns(self) -> None:
        conn = self._ensure_connection()
        query: str
        params: tuple[object, ...]
        if self.config.schema:
            query = (
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s"
            )
            params = (self.config.schema, self.config.table)
        else:
            query = "SELECT column_name FROM information_schema.columns WHERE table_name = %s"
            params = (self.config.table,)
        with conn.cursor() as cur:
            cur.execute(query, params)
            self._available_columns = {row[0] for row in cur.fetchall()}
        self._resolve_columns()

    def _resolve_columns(self) -> None:
        columns = self._available_columns or set()
        required = {
            self.config.timestamp_column,
            self.config.symbol_column,
            self.config.asset_id_column,
        }
        missing = sorted(col for col in required if col not in columns)
        if missing:
            raise RuntimeError(
                f"Faltan columnas requeridas {missing} en {self.config.schema}.{self.config.table}"
            )
        if self._resolved_price_column and self._resolved_price_column not in columns:
            raise RuntimeError(
                f"La columna de precio '{self._resolved_price_column}' no existe en la tabla"
            )
        if not self._resolved_price_column:
            for candidate in self.config.price_candidates:
                if candidate in columns:
                    self._resolved_price_column = candidate
                    break
        if not self._resolved_price_column:
            raise RuntimeError(
                "No se encontró ninguna columna de precio válida; configure POSTGRES_PRICE_COLUMN"
            )
        self.logger.info(
            "Columna de precio seleccionada: %s", self._resolved_price_column
        )

    def session_starts_for_weekday(self, weekday: int, limit: int) -> List[datetime]:
        conn = self._ensure_connection()
        assert sql is not None
        effective_limit = max(limit * self.config.oversample_multiplier, limit)
        query = sql.SQL(
            """
            SELECT date_trunc('day', {ts_col}) AS session_start
            FROM {table}
            WHERE {symbol_col} = %s
              AND {asset_id_col} = %s
              AND EXTRACT(ISODOW FROM {ts_col}) = %s
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT %s
            """
        ).format(
            ts_col=sql.Identifier(self.config.timestamp_column),
            table=self._table_identifier(),
            symbol_col=sql.Identifier(self.config.symbol_column),
            asset_id_col=sql.Identifier(self.config.asset_id_column),
        )
        params = (
            self.config.symbol,
            self.config.asset_id,
            weekday + 1,
            effective_limit,
        )
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        session_starts: List[datetime] = []
        for row in rows:
            session_start = row[0]
            if isinstance(session_start, datetime):
                if session_start.tzinfo is None:
                    session_start = session_start.replace(tzinfo=UTC)
                else:
                    session_start = session_start.astimezone(UTC)
                session_starts.append(session_start)
        return session_starts

    def fetch_session_dataframe(self, session_start: datetime) -> Optional[pd.DataFrame]:
        conn = self._ensure_connection()
        assert sql is not None
        if not self._resolved_price_column:
            self._resolve_columns()
        assert self._resolved_price_column is not None
        start_utc = session_start.astimezone(UTC) if session_start.tzinfo else session_start.replace(tzinfo=UTC)
        end_utc = start_utc + timedelta(days=1)
        cursor_kwargs = {"row_factory": dict_row} if dict_row is not None else {}
        query = sql.SQL(
            """
            SELECT {ts_col} AS ts, {price_col} AS price
            FROM {table}
            WHERE {symbol_col} = %s
              AND {asset_id_col} = %s
              AND {ts_col} >= %s
              AND {ts_col} < %s
            ORDER BY {ts_col} ASC
            """
        ).format(
            ts_col=sql.Identifier(self.config.timestamp_column),
            price_col=sql.Identifier(self._resolved_price_column),
            table=self._table_identifier(),
            symbol_col=sql.Identifier(self.config.symbol_column),
            asset_id_col=sql.Identifier(self.config.asset_id_column),
        )
        params = (
            self.config.symbol,
            self.config.asset_id,
            start_utc,
            end_utc,
        )
        with conn.cursor(**cursor_kwargs) as cur:  # type: ignore[arg-type]
            cur.execute(query, params)
            rows = cur.fetchall()
        if not rows:
            self.logger.warning("Sesión sin datos válidos el %s", start_utc.date())
            return None
        if dict_row is not None:
            df = pd.DataFrame(rows)
        else:
            df = pd.DataFrame(rows, columns=["ts", "price"])
        if df.empty:
            self.logger.warning("Sesión sin datos válidos el %s", start_utc.date())
            return None
        df = df.rename(columns={"ts": "timestamp", "price": "close"})
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["timestamp", "close"])
        if df.empty:
            self.logger.warning(
                "Sesión sin precios utilizables el %s (tras limpiar valores nulos)",
                start_utc.date(),
            )
            return None
        return _normalize_intraday_dataframe(df, start_utc, self.logger)

def setup_logging(log_level: str = "INFO") -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return logging.getLogger("capital_intradia")


def get_credentials() -> CapitalCredentials:
    load_dotenv()
    api_key = os.getenv("CAPITAL_API_KEY", "").strip()
    email = os.getenv("CAPITAL_EMAIL", "").strip()
    password = os.getenv("CAPITAL_PASSWORD", "").strip()
    if not api_key or not email or not password:
        raise CapitalAPIError("Credenciales incompletas en .env (CAPITAL_API_KEY, CAPITAL_EMAIL, CAPITAL_PASSWORD)")
    return CapitalCredentials(api_key=api_key, email=email, password=password)


def load_postgres_intraday_config(logger: logging.Logger) -> Optional[PostgresIntradayConfig]:
    load_dotenv()
    host = os.getenv("POSTGRES_HOST")
    if not host:
        return None
    try:
        port = int(os.getenv("POSTGRES_PORT", "5432"))
    except ValueError as exc:  # pragma: no cover - configuración inválida
        raise RuntimeError("POSTGRES_PORT debe ser numérico") from exc
    database = os.getenv("POSTGRES_DB") or os.getenv("POSTGRES_DATABASE")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    if not database or not user or not password:
        raise RuntimeError(
            "Config de PostgreSQL incompleta: se requieren POSTGRES_DB, POSTGRES_USER y POSTGRES_PASSWORD"
        )
    symbol = os.getenv("POSTGRES_US100_SYMBOL", "US100").strip() or "US100"
    asset_id_raw = os.getenv("POSTGRES_US100_ASSET_ID", "97").strip() or "97"
    try:
        asset_id = int(asset_id_raw)
    except ValueError as exc:  # pragma: no cover - configuración inválida
        raise RuntimeError("POSTGRES_US100_ASSET_ID debe ser numérico") from exc
    schema = os.getenv("POSTGRES_SCHEMA", "public").strip()
    table = os.getenv("POSTGRES_INTRADAY_TABLE", "cotizaciones_intradia_cfd").strip() or "cotizaciones_intradia_cfd"
    symbol_column = os.getenv("POSTGRES_SYMBOL_COLUMN", "symbol").strip() or "symbol"
    asset_id_column = os.getenv("POSTGRES_ASSET_ID_COLUMN", "asset_id").strip() or "asset_id"
    timestamp_column = os.getenv("POSTGRES_TIMESTAMP_COLUMN", "timestamp").strip() or "timestamp"
    price_column = os.getenv("POSTGRES_PRICE_COLUMN")
    price_candidates_env = os.getenv("POSTGRES_PRICE_CANDIDATES")
    price_candidates = (
        [col.strip() for col in price_candidates_env.split(",") if col.strip()]
        if price_candidates_env
        else None
    )
    try:
        connect_timeout = int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "10"))
    except ValueError as exc:  # pragma: no cover - configuración inválida
        raise RuntimeError("POSTGRES_CONNECT_TIMEOUT debe ser numérico") from exc
    try:
        oversample_multiplier = int(os.getenv("POSTGRES_SESSION_OVERSAMPLE", "3"))
    except ValueError as exc:  # pragma: no cover - configuración inválida
        raise RuntimeError("POSTGRES_SESSION_OVERSAMPLE debe ser numérico") from exc
    if oversample_multiplier < 1:
        logger.warning("POSTGRES_SESSION_OVERSAMPLE < 1, se ajusta a 1")
        oversample_multiplier = 1
    config = PostgresIntradayConfig(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        symbol=symbol,
        asset_id=asset_id,
        schema=schema,
        table=table,
        symbol_column=symbol_column,
        asset_id_column=asset_id_column,
        timestamp_column=timestamp_column,
        price_column=price_column.strip() if price_column and price_column.strip() else None,
        connect_timeout=connect_timeout,
        oversample_multiplier=oversample_multiplier,
    )
    if price_candidates:
        config.price_candidates = price_candidates
    return config


def find_us100_epic(client: CapitalClient) -> str:
    markets = client.search_epic("US100")
    candidates = []
    for item in markets:
        epic = item.get("epic")
        instrument = item.get("instrument", {}) if isinstance(item, dict) else {}
        symbol = instrument.get("symbol") if isinstance(instrument, dict) else item.get("symbol")
        name = instrument.get("name") if isinstance(instrument, dict) else item.get("name")
        if epic and ((symbol and symbol.upper() == "US100") or (name and "US 100" in name.upper())):
            candidates.append((epic, symbol or name or ""))
    if not candidates and markets:
        for item in markets:
            epic = item.get("epic")
            if epic:
                candidates.append((epic, item.get("symbol") or item.get("name") or ""))
    if not candidates:
        raise CapitalAPIError("No se encontró EPIC para US100")
    chosen = candidates[0][0]
    client.logger.info("EPIC seleccionado para US100: %s", chosen)
    return chosen


def normalize_price_record(record: Dict[str, object]) -> Optional[Dict[str, object]]:
    timestamp = record.get("snapshotTimeUTC") or record.get("snapshotTime")
    if not timestamp:
        return None
    try:
        ts = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    except ValueError:
        return None

    def _mid_price(price_field: str) -> Optional[float]:
        value = record.get(price_field)
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, dict):
            numbers = [float(v) for v in value.values() if isinstance(v, (int, float))]
            if numbers:
                return sum(numbers) / len(numbers)
        return None

    close_price = _mid_price("closePrice")
    if close_price is None:
        close_price = _mid_price("lastTradedPrice")
    if close_price is None:
        open_price = _mid_price("openPrice")
        if open_price is not None:
            close_price = open_price
    if close_price is None:
        return None

    return {"timestamp": ts.replace(tzinfo=ts.tzinfo or UTC), "close": close_price}


def daterange_for_weekday(target_weekday: int, sessions: int) -> List[datetime]:
    today = datetime.now(tz=UTC).date()
    dates: List[datetime] = []
    cursor = today
    while len(dates) < sessions:
        if cursor.weekday() == target_weekday:
            dates.append(datetime.combine(cursor, datetime.min.time(), tzinfo=UTC))
        cursor -= timedelta(days=1)
        if cursor < today - timedelta(days=400):  # límite de seguridad
            break
    return dates


def _normalize_intraday_dataframe(
    df: pd.DataFrame, session_start: datetime, logger: logging.Logger
) -> Optional[pd.DataFrame]:
    if df.empty:
        logger.warning("Sesión sin datos válidos el %s", session_start.date())
        return None
    session_start_utc = session_start.astimezone(UTC) if session_start.tzinfo else session_start.replace(
        tzinfo=UTC
    )
    df = df.copy()
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["timestamp", "close"])
    if df.empty:
        logger.warning(
            "Sesión sin precios utilizables el %s (tras limpiar valores nulos)",
            session_start_utc.date(),
        )
        return None
    df["minute"] = (df["timestamp"] - session_start_utc).dt.total_seconds() / 60.0
    df = df[(df["minute"] >= 0) & (df["minute"] <= 24 * 60)]
    if df.empty:
        logger.warning(
            "Sesión fuera de rango temporal esperado el %s (sin minutos en ventana 00:00-24:00)",
            session_start_utc.date(),
        )
        return None
    df["minute"] = df["minute"].round().astype(int)
    df = df.groupby("minute", as_index=False)["close"].mean()
    logger.debug(
        "Normalizada sesión %s con %s minutos válidos", session_start_utc.date(), len(df)
    )
    return df


def build_intraday_dataframe(
    client: CapitalClient,
    epic: str,
    session_start: datetime,
    resolution: str = "MINUTE",
) -> Optional[pd.DataFrame]:
    session_end = session_start + timedelta(days=1)
    raw_prices = client.fetch_prices(epic, session_start, session_end, resolution=resolution)
    normalized = [normalize_price_record(item) for item in raw_prices]
    normalized = [item for item in normalized if item]
    if not normalized:
        client.logger.warning("Sesión sin datos válidos el %s", session_start.date())
        return None
    df = pd.DataFrame(normalized)
    return _normalize_intraday_dataframe(df, session_start, client.logger)


def compute_average_intraday(
    weekday: int,
    sessions: int,
    session_loader: Callable[[datetime], Optional[pd.DataFrame]],
    logger: logging.Logger,
    session_provider: Optional[Callable[[int, int], List[datetime]]] = None,
) -> pd.DataFrame:
    if session_provider:
        session_candidates = session_provider(weekday, sessions)
    else:
        session_candidates = daterange_for_weekday(weekday, sessions)
    if not session_candidates:
        raise CapitalAPIError(f"No se encontraron fechas para el weekday {weekday}")

    frames: List[pd.DataFrame] = []
    for start in session_candidates:
        if len(frames) >= sessions:
            break
        df = session_loader(start)
        if df is None:
            continue
        frames.append(df)

    if not frames:
        raise CapitalAPIError(f"No se obtuvieron datos válidos para weekday {weekday}")

    if len(frames) < sessions:
        logger.warning(
            "Sólo se pudieron utilizar %s de las %s sesiones requeridas para el día %s",
            len(frames),
            sessions,
            weekday,
        )

    master_index = pd.Index(range(0, 24 * 60), name="minute")
    aligned = []
    for df in frames:
        aligned_df = df.set_index("minute").reindex(master_index)
        aligned.append(aligned_df)
    stacked = pd.concat(aligned, axis=1)
    averaged = stacked.mean(axis=1, skipna=True).to_frame(name="average_close")
    averaged.reset_index(inplace=True)
    averaged["time"] = averaged["minute"].apply(
        lambda m: (datetime.min + timedelta(minutes=int(m))).time()
    )
    averaged["sessions_used"] = stacked.count(axis=1)
    return averaged


def _minute_to_hhmm(minute: int) -> str:
    hours, minutes = divmod(int(minute), 60)
    return f"{hours:02d}:{minutes:02d}"


def build_weekday_profiles(
    logger: logging.Logger,
    trading_weekdays: Sequence[int],
    session_loader: Callable[[datetime], Optional[pd.DataFrame]],
    session_provider: Optional[Callable[[int, int], List[datetime]]],
    sessions: int,
) -> Dict[int, pd.DataFrame]:
    profiles: Dict[int, pd.DataFrame] = {}
    for weekday in range(7):
        if weekday not in trading_weekdays:
            logger.info("Se omite el día %s por no contar con cotizaciones de mercado", weekday)
            profiles[weekday] = pd.DataFrame(
                columns=["minute", "average_close", "time", "sessions_used"]
            )
            continue
        try:
            logger.info("Procesando día %s", weekday)
            profile = compute_average_intraday(
                weekday=weekday,
                sessions=sessions,
                session_loader=session_loader,
                logger=logger,
                session_provider=session_provider,
            )
            valid_points = profile["sessions_used"]
            coverage = (valid_points >= 1).sum()
            total_points = profile.shape[0]
            coverage_pct = (coverage / total_points * 100.0) if total_points else 0.0
            logger.info(
                "Día %s: puntos con datos %s/%s (%.1f%%)",
                weekday,
                coverage,
                total_points,
                coverage_pct,
            )
            if coverage == 0:
                logger.warning(
                    "Día %s sin minutos válidos: el mercado estuvo cerrado en esa ventana UTC",
                    weekday,
                )
            elif coverage_pct < 50.0:
                valid_minutes = profile.loc[valid_points >= 1, "minute"].astype(int)
                first_minute = valid_minutes.min()
                last_minute = valid_minutes.max()
                logger.warning(
                    "Cobertura limitada el día %s: datos entre %s y %s UTC."
                    " Posible sesión parcial o cierre de mercado.",
                    weekday,
                    _minute_to_hhmm(first_minute),
                    _minute_to_hhmm(last_minute),
                )
            profiles[weekday] = profile
        except CapitalAPIError as exc:
            logger.warning("No se pudo calcular el día %s: %s", weekday, exc)
            profiles[weekday] = pd.DataFrame(
                columns=["minute", "average_close", "time", "sessions_used"]
            )
        except Exception as exc:  # pragma: no cover - errores inesperados
            logger.error("Fallo inesperado calculando el día %s: %s", weekday, exc)
            profiles[weekday] = pd.DataFrame(
                columns=["minute", "average_close", "time", "sessions_used"]
            )
    return profiles


def plot_weekday_profiles(profiles: Dict[int, pd.DataFrame], output_path: Path) -> None:
    import matplotlib.pyplot as plt

    colors = {
        0: "#1f77b4",  # lunes
        1: "#ff7f0e",
        2: "#2ca02c",
        3: "#d62728",
        4: "#9467bd",
        5: "#8c564b",
        6: "#e377c2",
    }
    weekday_names = [
        "Lunes",
        "Martes",
        "Miércoles",
        "Jueves",
        "Viernes",
        "Sábado",
        "Domingo",
    ]

    plt.figure(figsize=(12, 6))
    for weekday, df in sorted(profiles.items()):
        if df.empty:
            continue
        times = df["minute"] / 60.0
        plt.plot(times, df["average_close"], label=weekday_names[weekday], color=colors.get(weekday))
    plt.title("Promedio intradía US100 por día de la semana (últimas 50 sesiones)")
    plt.xlabel("Horas desde la apertura (UTC)")
    plt.ylabel("Precio promedio (close)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path)
    plt.close()


def main() -> None:
    logger = setup_logging()
    try:
        sessions = int(os.getenv("INTRADAY_AVG_SESSIONS", "50"))
    except ValueError:
        logger.warning("INTRADAY_AVG_SESSIONS inválido, se usará el valor por defecto (50)")
        sessions = 50
    if sessions <= 0:
        logger.warning("INTRADAY_AVG_SESSIONS debe ser positivo, se ajusta a 50")
        sessions = 50
    trading_weekdays = tuple(
        int(x)
        for x in os.getenv("CAPITAL_TRADING_WEEKDAYS", "0,1,2,3,4").split(",")
        if x.strip().isdigit()
    )
    if not trading_weekdays:
        trading_weekdays = (0, 1, 2, 3, 4)
    postgres_config = load_postgres_intraday_config(logger)
    profiles: Dict[int, pd.DataFrame]
    if postgres_config:
        logger.info(
            "Usando origen PostgreSQL para US100 (symbol=%s, asset_id=%s)",
            postgres_config.symbol,
            postgres_config.asset_id,
        )
        with PostgresIntradaySource(postgres_config, logger) as source:
            profiles = build_weekday_profiles(
                logger=logger,
                trading_weekdays=trading_weekdays,
                session_loader=source.fetch_session_dataframe,
                session_provider=source.session_starts_for_weekday,
                sessions=sessions,
            )
    else:
        logger.info("No se configuró PostgreSQL; se usará la API de Capital.com")
        credentials = get_credentials()
        cache_dir = Path("cache_responses")
        client = CapitalClient(
            credentials=credentials,
            logger=logger,
            request_timeout=int(os.getenv("CAPITAL_REQUEST_TIMEOUT", "30")),
            max_requests_per_minute=int(os.getenv("CAPITAL_MAX_REQUESTS_PER_MINUTE", "20")),
            cache_dir=cache_dir,
        )
        client.authenticate()
        epic = os.getenv("CAPITAL_US100_EPIC")
        if not epic:
            epic = find_us100_epic(client)
        profiles = build_weekday_profiles(
            logger=logger,
            trading_weekdays=trading_weekdays,
            session_loader=lambda start: build_intraday_dataframe(
                client, epic, start, resolution="MINUTE"
            ),
            session_provider=None,
            sessions=sessions,
        )
    output_path = Path("outputs/us100_intradia_promedios.png")
    plot_weekday_profiles(profiles, output_path)
    logger.info("Gráfico generado en %s", output_path.resolve())


if __name__ == "__main__":
    try:
        main()
    except CapitalAPIError as exc:
        logging.getLogger("capital_intradia").error("Ejecución detenida: %s", exc)
        raise
