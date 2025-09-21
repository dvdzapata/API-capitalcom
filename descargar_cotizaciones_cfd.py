#!/usr/bin/env python3
"""Descarga y almacena históricos de CFDs desde Capital.com en PostgreSQL.

El script recupera todos los assets CFD con epic definido en la tabla
``assets`` (o la configurada vía variables de entorno), consulta las
cotizaciones históricas disponibles en la API de Capital.com y guarda los
registros nuevos en las tablas ``cotizaciones_diarias_cfd`` y
``cotizaciones_intradia_cfd`` de la base ``financial_data_v2`` (por defecto).

Características destacadas
-------------------------
* Sesiones resilientes frente a expiración (renovación automática cada 8 min).
* Reintentos exponenciales y limitación de velocidad a < 5 peticiones/seg.
* Descarga incremental: arranca desde el último registro en base.
* Validación básica de datos (orden de precios, spreads, timestamps).
* Inserciones idempotentes con ON CONFLICT y transacciones acotadas.
* Registro estructurado en consola para facilitar el monitoreo.

Requisitos: ``requests``, ``psycopg2-binary`` y ``python-dotenv`` (opcional).
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Collection, Dict, Iterable, List, Optional, Sequence

import psycopg2
import psycopg2.extras
import requests

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - dependencia opcional para entornos mínimos
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default

CAPITAL_API_BASE = "https://api-capital.backend-capital.com/api/v1"
UTC = dt.timezone.utc
DEFAULT_SOURCE = "capital.com"
DEFAULT_ASSETS_TABLE = os.environ.get("CAPITAL_ASSETS_TABLE", "assets").strip() or "assets"
ASSET_ID_COLUMN = os.environ.get("CAPITAL_ASSET_ID_COLUMN", "asset_id").strip() or "asset_id"
DEFAULT_DAILY_TABLE = os.environ.get("CAPITAL_DAILY_TABLE", "cotizaciones_diarias_cfd")
DEFAULT_INTRADAY_TABLE = os.environ.get("CAPITAL_INTRADAY_TABLE", "cotizaciones_intradia_cfd")
DEFAULT_START_DATE = os.environ.get("CAPITAL_START_DATE", "2000-01-01T00:00:00")
DEFAULT_INTRADAY_START = os.environ.get("CAPITAL_INTRADAY_START", DEFAULT_START_DATE)
MAX_POINTS_PER_CALL = 1000
REQUEST_THROTTLE_SECONDS = _env_float("CAPITAL_REQUEST_THROTTLE", 0.25)
SESSION_REFRESH_MINUTES = 8
SESSION_TTL_MINUTES = 10
MAX_REQUEST_RETRIES = 4
LOGIN_TIMEOUT_SECONDS = _env_float("CAPITAL_LOGIN_TIMEOUT", 30.0)
LOGIN_MAX_RETRIES = _env_int("CAPITAL_LOGIN_MAX_RETRIES", 5)
LOGIN_RETRY_BASE_SECONDS = _env_float("CAPITAL_LOGIN_RETRY_BASE", 2.0)
LOGIN_RETRY_MAX_SECONDS = _env_float("CAPITAL_LOGIN_RETRY_MAX", 30.0)
FINE_GRAIN_SYMBOLS = {"AVGO", "AMD", "NVDA"}


@dataclass(frozen=True)
class Asset:
    asset_id: int
    epic: str
    symbol: Optional[str]


@dataclass(frozen=True)
class Timeframe:
    name: str
    resolution: str
    delta: dt.timedelta
    table: str
    intervalo: str


TIMEFRAMES: Sequence[Timeframe] = (
    Timeframe(
        name="DAY",
        resolution="DAY",
        delta=dt.timedelta(days=1),
        table=DEFAULT_DAILY_TABLE,
        intervalo="1D",
    ),
    Timeframe(
        name="MINUTE",
        resolution="MINUTE",
        delta=dt.timedelta(minutes=1),
        table=DEFAULT_INTRADAY_TABLE,
        intervalo="1m",
    ),
    Timeframe(
        name="MINUTE_5",
        resolution="MINUTE_5",
        delta=dt.timedelta(minutes=5),
        table=DEFAULT_INTRADAY_TABLE,
        intervalo="5m",
    ),
    Timeframe(
        name="MINUTE_15",
        resolution="MINUTE_15",
        delta=dt.timedelta(minutes=15),
        table=DEFAULT_INTRADAY_TABLE,
        intervalo="15m",
    ),
    Timeframe(
        name="MINUTE_30",
        resolution="MINUTE_30",
        delta=dt.timedelta(minutes=30),
        table=DEFAULT_INTRADAY_TABLE,
        intervalo="30m",
    ),
    Timeframe(
        name="HOUR",
        resolution="HOUR",
        delta=dt.timedelta(hours=1),
        table=DEFAULT_INTRADAY_TABLE,
        intervalo="1h",
    ),
    Timeframe(
        name="WEEK",
        resolution="WEEK",
        delta=dt.timedelta(weeks=1),
        table=DEFAULT_INTRADAY_TABLE,
        intervalo="1w",
    ),
)


class CapitalAPIClient:
    """Cliente resiliente para Capital.com."""

    def __init__(
        self,
        api_key: str,
        identifier: str,
        password: str,
        session: Optional[requests.Session] = None,
    ) -> None:
        if not api_key or not identifier or not password:
            raise ValueError("API key, email y password son obligatorios")
        self.api_key = api_key
        self.identifier = identifier
        self.password = password
        self.session = session or requests.Session()
        self.last_login: Optional[dt.datetime] = None
        self.last_request_mono: float = 0.0
        self.session.headers.update(
            {
                "X-CAP-API-KEY": self.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def ensure_session(self, force: bool = False) -> None:
        now = dt.datetime.now(UTC)
        if not force and self.last_login is not None:
            age = now - self.last_login
            if age < dt.timedelta(minutes=SESSION_REFRESH_MINUTES):
                return
        self._login()

    def _login(self) -> None:
        payload = {
            "identifier": self.identifier,
            "password": self.password,
            "encryptedPassword": False,
        }
        last_exception: Optional[BaseException] = None
        response: Optional[requests.Response] = None
        attempts = max(1, LOGIN_MAX_RETRIES)
        for attempt in range(1, attempts + 1):
            logging.info(
                "Iniciando sesión en Capital.com (intento %s/%s)",
                attempt,
                attempts,
            )
            try:
                response = self.session.post(
                    f"{CAPITAL_API_BASE}/session",
                    json=payload,
                    timeout=LOGIN_TIMEOUT_SECONDS,
                )
            except requests.RequestException as exc:  # pragma: no cover - defensivo
                last_exception = exc
                logging.warning(
                    "No fue posible abrir sesión (%s/%s): %s",
                    attempt,
                    attempts,
                    exc,
                )
                if attempt >= attempts:
                    logging.error("Agotados los reintentos de login en Capital.com")
                    raise
                sleep_seconds = min(
                    LOGIN_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
                    LOGIN_RETRY_MAX_SECONDS,
                )
                time.sleep(max(0.0, sleep_seconds))
                continue
            if response.status_code in (200, 201):
                break
            if response.status_code in (401, 403):
                logging.error(
                    "Credenciales rechazadas (%s): %s",
                    response.status_code,
                    response.text[:400],
                )
                response.raise_for_status()
            logging.warning(
                "Login respondió %s (intento %s/%s): %s",
                response.status_code,
                attempt,
                attempts,
                response.text[:400],
            )
            if attempt >= attempts:
                response.raise_for_status()
            sleep_seconds = min(
                LOGIN_RETRY_BASE_SECONDS * (2 ** (attempt - 1)),
                LOGIN_RETRY_MAX_SECONDS,
            )
            time.sleep(max(0.0, sleep_seconds))
        else:  # pragma: no cover - salvaguarda
            if last_exception is not None:
                raise last_exception
            raise RuntimeError("No se pudo iniciar sesión en Capital.com")
        if response is None:  # pragma: no cover - defensivo
            raise RuntimeError("Respuesta vacía al iniciar sesión en Capital.com")
        cst = response.headers.get("CST")
        xst = response.headers.get("X-SECURITY-TOKEN")
        if not cst or not xst:
            # Intentar recuperar del cuerpo por si cambian la cabecera.
            try:
                body = response.json()
            except ValueError:
                body = {}
            cst = cst or body.get("CST") or body.get("cst")
            xst = xst or body.get("securityToken") or body.get("X-SECURITY-TOKEN")
        if not cst or not xst:
            raise RuntimeError("Capital.com no devolvió tokens CST/X-SECURITY-TOKEN")
        self.session.headers.update({"CST": cst, "X-SECURITY-TOKEN": xst})
        self.last_login = dt.datetime.now(UTC)
        logging.info("Sesión establecida (expira en %s min)", SESSION_TTL_MINUTES)

    def _throttle(self) -> None:
        if REQUEST_THROTTLE_SECONDS <= 0:
            return
        elapsed = time.monotonic() - self.last_request_mono
        sleep_needed = REQUEST_THROTTLE_SECONDS - elapsed
        if sleep_needed > 0:
            time.sleep(sleep_needed)

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, str]] = None,
        expected_error_codes: Optional[Dict[int, Collection[str]]] = None,
    ) -> requests.Response:
        self.ensure_session()
        url = f"{CAPITAL_API_BASE}{path}"
        for attempt in range(1, MAX_REQUEST_RETRIES + 1):
            self._throttle()
            try:
                response = self.session.request(
                    method,
                    url,
                    params=params,
                    timeout=30,
                )
                self.last_request_mono = time.monotonic()
            except requests.RequestException as exc:
                logging.warning("Error de red (%s/%s): %s", attempt, MAX_REQUEST_RETRIES, exc)
                time.sleep(min(2 ** attempt, 30))
                continue
            if response.status_code == 401 and attempt < MAX_REQUEST_RETRIES:
                logging.info("Sesión expirada, renovando tokens")
                self.ensure_session(force=True)
                continue
            if response.status_code in (429, 502, 503, 504) and attempt < MAX_REQUEST_RETRIES:
                logging.warning(
                    "Respuesta %s en %s (intento %s/%s)",
                    response.status_code,
                    url,
                    attempt,
                    MAX_REQUEST_RETRIES,
                )
                time.sleep(min(2 ** attempt, 30))
                continue
            if not response.ok:
                if expected_error_codes:
                    allowed_codes = expected_error_codes.get(response.status_code)
                    if allowed_codes is not None:
                        error_code = ""
                        try:
                            error_payload = response.json()
                        except ValueError:
                            error_payload = {}
                        if isinstance(error_payload, dict):
                            error_code = str(error_payload.get("errorCode") or "")
                        if (
                            not allowed_codes
                            or "*" in allowed_codes
                            or error_code in allowed_codes
                            or (error_code == "" and "" in allowed_codes)
                        ):
                            logging.debug(
                                "Respuesta %s permitida en %s (errorCode=%s)",
                                response.status_code,
                                url,
                                error_code or "<vacío>",
                            )
                            return response
                logging.error("Respuesta no OK (%s): %s", response.status_code, response.text[:400])
                response.raise_for_status()
            return response
        raise RuntimeError(f"No se pudo obtener {url} tras {MAX_REQUEST_RETRIES} intentos")

    def get_prices(
        self,
        epic: str,
        resolution: str,
        start: Optional[dt.datetime],
        end: Optional[dt.datetime],
        max_points: int = MAX_POINTS_PER_CALL,
    ) -> Dict[str, object]:
        params: Dict[str, str] = {
            "resolution": resolution,
            "max": str(max(1, min(max_points, MAX_POINTS_PER_CALL))),
        }
        if start is not None:
            params["from"] = datetime_to_api_param(start)
        if end is not None:
            params["to"] = datetime_to_api_param(end)
        response = self._request(
            "GET",
            f"/prices/{epic}",
            params=params,
            expected_error_codes={
                400: ("error.invalid.from", "error.invalid.to", "error.invalid.query"),
                404: ("*", "error.prices.not-found", "error.prices.no-prices"),
            },
        )
        if response.status_code == 400:
            error_code = ""
            error_message = response.text[:400]
            try:
                body = response.json()
            except ValueError:
                body = {}
            if isinstance(body, dict):
                error_code = str(body.get("errorCode") or "")
                if body:
                    error_message = str(body)[:400]
            logging.warning(
                "Petición rechazada %s %s (%s - %s): %s",
                epic,
                resolution,
                params.get("from", "?"),
                params.get("to", "?"),
                error_code or error_message,
            )
            return {"prices": []}
        if response.status_code == 404:
            error_code = ""
            error_message = response.text[:400]
            try:
                body = response.json()
            except ValueError:
                body = {}
            if isinstance(body, dict):
                error_code = str(body.get("errorCode") or "")
                if body:
                    error_message = str(body)[:400]
            logging.warning(
                "Sin precios %s %s (%s - %s): %s",
                epic,
                resolution,
                params.get("from", "?"),
                params.get("to", "?"),
                error_code or error_message,
            )
            return {"prices": []}
        try:
            payload = response.json()
        except ValueError as exc:
            logging.error("Respuesta JSON inválida: %s", exc)
            raise
        if isinstance(payload, dict) and payload.get("prices") is not None:
            return payload
        logging.error("Respuesta inesperada para %s: %s", epic, payload)
        raise RuntimeError("Capital.com devolvió un payload sin 'prices'")


def parse_datetime(value: str, description: str) -> dt.datetime:
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{description} inválido: {value}") from exc
    return ensure_utc_datetime(parsed)


def ensure_utc_datetime(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def datetime_to_api_param(value: dt.datetime) -> str:
    """Formatea fechas en UTC sin sufijo horario para Capital.com."""

    value_utc = ensure_utc_datetime(value).replace(microsecond=0)
    return value_utc.strftime("%Y-%m-%dT%H:%M:%S")


def align_floor(value: dt.datetime, timeframe: Timeframe) -> dt.datetime:
    value_utc = ensure_utc_datetime(value).replace(microsecond=0)
    if timeframe.resolution == "WEEK":
        start_of_week = value_utc - dt.timedelta(days=value_utc.weekday())
        return start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    delta_seconds = int(timeframe.delta.total_seconds())
    if delta_seconds <= 0:
        return value_utc
    epoch = int(value_utc.timestamp())
    aligned_epoch = (epoch // delta_seconds) * delta_seconds
    return dt.datetime.fromtimestamp(aligned_epoch, tz=UTC)


def to_decimal(value: Optional[object]) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError):
        return None


def average_bid_ask(bid: Optional[Decimal], ask: Optional[Decimal]) -> Optional[Decimal]:
    values = [v for v in (bid, ask) if v is not None]
    if not values:
        return None
    return sum(values) / Decimal(len(values))


def compute_spread(bid: Optional[Decimal], ask: Optional[Decimal]) -> Optional[Decimal]:
    if bid is None or ask is None:
        return None
    return ask - bid


def parse_snapshot_time(price_entry: Dict[str, object]) -> Optional[dt.datetime]:
    raw = price_entry.get("snapshotTimeUTC") or price_entry.get("snapshotTime")
    if not isinstance(raw, str):
        return None
    try:
        snap = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return ensure_utc_datetime(snap)


def allows_fine_grain(asset: Asset) -> bool:
    symbol = (asset.symbol or "").upper()
    epic = (asset.epic or "").upper()
    epic_base = epic.split(".", 1)[0]
    return any(
        candidate in FINE_GRAIN_SYMBOLS
        for candidate in (symbol, epic, epic_base)
        if candidate
    )


def validate_bar(bar: Dict[str, object]) -> bool:
    high_candidates = [bar.get("high"), bar.get("high_bid"), bar.get("high_ask")]
    low_candidates = [bar.get("low"), bar.get("low_bid"), bar.get("low_ask")]
    highs = [c for c in high_candidates if isinstance(c, Decimal)]
    lows = [c for c in low_candidates if isinstance(c, Decimal)]
    if highs and lows:
        if min(highs) < max(lows):
            return False
        if max(lows) > max(highs):
            return False
    for spread_key in ("spread_open", "spread_close"):
        spread = bar.get(spread_key)
        if isinstance(spread, Decimal) and spread < 0:
            return False
    return True


def rows_from_prices(
    asset: Asset,
    timeframe: Timeframe,
    prices: Iterable[Dict[str, object]],
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    created_at = dt.datetime.now(UTC).replace(microsecond=0)
    for entry in prices:
        snap = parse_snapshot_time(entry)
        if not snap:
            logging.debug("Entrada sin snapshotTime ignorada: %s", entry)
            continue
        snap_utc = ensure_utc_datetime(snap)
        open_price = entry.get("openPrice", {}) if isinstance(entry, dict) else {}
        close_price = entry.get("closePrice", {}) if isinstance(entry, dict) else {}
        high_price = entry.get("highPrice", {}) if isinstance(entry, dict) else {}
        low_price = entry.get("lowPrice", {}) if isinstance(entry, dict) else {}
        open_bid = to_decimal(open_price.get("bid")) if isinstance(open_price, dict) else None
        open_ask = to_decimal(open_price.get("ask")) if isinstance(open_price, dict) else None
        close_bid = to_decimal(close_price.get("bid")) if isinstance(close_price, dict) else None
        close_ask = to_decimal(close_price.get("ask")) if isinstance(close_price, dict) else None
        high_bid = to_decimal(high_price.get("bid")) if isinstance(high_price, dict) else None
        high_ask = to_decimal(high_price.get("ask")) if isinstance(high_price, dict) else None
        low_bid = to_decimal(low_price.get("bid")) if isinstance(low_price, dict) else None
        low_ask = to_decimal(low_price.get("ask")) if isinstance(low_price, dict) else None
        volume_raw = entry.get("lastTradedVolume")
        volume: Optional[int]
        if isinstance(volume_raw, (int, float, Decimal)):
            volume = int(float(volume_raw))
        elif isinstance(volume_raw, str):
            try:
                volume = int(float(volume_raw))
            except ValueError:
                volume = None
        else:
            volume = None
        row = {
            "asset_id": asset.asset_id,
            "fecha": snap_utc,
            "open_bid": open_bid,
            "open_ask": open_ask,
            "open": average_bid_ask(open_bid, open_ask),
            "high_bid": high_bid,
            "high_ask": high_ask,
            "high": average_bid_ask(high_bid, high_ask),
            "low_bid": low_bid,
            "low_ask": low_ask,
            "low": average_bid_ask(low_bid, low_ask),
            "close_bid": close_bid,
            "close_ask": close_ask,
            "close": average_bid_ask(close_bid, close_ask),
            "volumen_ultimo_trade": volume,
            "fuente": DEFAULT_SOURCE,
            "intervalo": timeframe.intervalo,
            "symbol": asset.symbol or asset.epic,
            "source": DEFAULT_SOURCE,
            "spread_open": compute_spread(open_bid, open_ask),
            "spread_close": compute_spread(close_bid, close_ask),
            "created_at": created_at,
            "ts_epoch": int(snap_utc.timestamp()),
        }
        if not validate_bar(row):
            logging.warning(
                "Barra inconsistente descartada (asset %s epic %s fecha %s)",
                asset.asset_id,
                asset.epic,
                row["fecha"],
            )
            continue
        rows.append(row)
    rows.sort(key=lambda r: r["fecha"])
    return rows


def load_assets(
    conn: psycopg2.extensions.connection,
    asset_ids: Optional[Sequence[int]] = None,
    epics: Optional[Sequence[str]] = None,
) -> List[Asset]:
    filters: List[str] = ["epic IS NOT NULL", "epic <> ''"]
    params: List[object] = []
    if asset_ids:
        filters.append(f"{ASSET_ID_COLUMN} = ANY(%s)")
        params.append(list(asset_ids))
    if epics:
        filters.append("epic = ANY(%s)")
        params.append(list(epics))
    where = " AND ".join(filters)
    query = (
        f"SELECT {ASSET_ID_COLUMN} AS asset_id, epic, symbol "
        f"FROM {DEFAULT_ASSETS_TABLE} WHERE {where} ORDER BY {ASSET_ID_COLUMN}"
    )
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    assets = [Asset(asset_id=row[0], epic=row[1], symbol=row[2]) for row in rows]
    logging.info("Assets obtenidos: %s", len(assets))
    return assets


def get_last_timestamp(
    conn: psycopg2.extensions.connection,
    table: str,
    asset_id: int,
    intervalo: Optional[str] = None,
) -> Optional[dt.datetime]:
    if intervalo is None:
        query = f"SELECT MAX(fecha) FROM {table} WHERE asset_id = %s"
        params = (asset_id,)
    else:
        query = f"SELECT MAX(fecha) FROM {table} WHERE asset_id = %s AND intervalo = %s"
        params = (asset_id, intervalo)
    with conn.cursor() as cur:
        cur.execute(query, params)
        value = cur.fetchone()[0]
    if isinstance(value, dt.datetime):
        return ensure_utc_datetime(value)
    return None


def insert_rows(
    conn: psycopg2.extensions.connection,
    table: str,
    rows: List[Dict[str, object]],
    conflict_cols: Sequence[str],
) -> int:
    if not rows:
        return 0
    columns = [
        "asset_id",
        "fecha",
        "open_bid",
        "open_ask",
        "open",
        "high_bid",
        "high_ask",
        "high",
        "low_bid",
        "low_ask",
        "low",
        "close_bid",
        "close_ask",
        "close",
        "volumen_ultimo_trade",
        "fuente",
        "intervalo",
        "symbol",
        "source",
        "spread_open",
        "spread_close",
        "created_at",
        "ts_epoch",
    ]
    values_template = psycopg2.extras.execute_values
    insert_query = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET "
        + ", ".join(f"{col} = EXCLUDED.{col}" for col in columns[2:])
    )
    data = [[row.get(col) for col in columns] for row in rows]
    with conn.cursor() as cur:
        values_template(cur, insert_query, data, page_size=200)
    conn.commit()
    return len(rows)


def fetch_and_store(
    conn: psycopg2.extensions.connection,
    client: CapitalAPIClient,
    asset: Asset,
    timeframe: Timeframe,
    start_date: dt.datetime,
    end_date: dt.datetime,
) -> int:
    last_fecha = get_last_timestamp(
        conn,
        table=timeframe.table,
        asset_id=asset.asset_id,
        intervalo=None if timeframe.table == DEFAULT_DAILY_TABLE else timeframe.intervalo,
    )
    effective_start = ensure_utc_datetime(start_date).replace(microsecond=0)
    effective_end = ensure_utc_datetime(end_date).replace(microsecond=0)
    if last_fecha:
        effective_start = max(effective_start, ensure_utc_datetime(last_fecha + timeframe.delta))
    effective_end = align_floor(effective_end, timeframe)
    if effective_start > effective_end:
        logging.info(
            "Asset %s (%s) %s al día. Último=%s",
            asset.asset_id,
            asset.epic,
            timeframe.name,
            last_fecha,
        )
        return 0
    current = align_floor(effective_start, timeframe)
    if current > effective_end:
        logging.info(
            "Asset %s (%s) %s al día. Último=%s",
            asset.asset_id,
            asset.epic,
            timeframe.name,
            last_fecha,
        )
        return 0
    logging.info(
        "Descargando %s %s desde %s hasta %s",
        asset.epic,
        timeframe.name,
        effective_start,
        effective_end,
    )
    total_inserted = 0
    while current <= effective_end:
        batch_limit = current + timeframe.delta * (MAX_POINTS_PER_CALL - 1)
        batch_end = align_floor(min(effective_end, batch_limit), timeframe)
        if batch_end < current:
            batch_end = current
        logging.debug(
            "Petición %s %s: %s -> %s",
            asset.epic,
            timeframe.name,
            current,
            batch_end,
        )
        try:
            payload = client.get_prices(asset.epic, timeframe.resolution, current, batch_end)
        except Exception as exc:
            logging.error(
                "Fallo obteniendo precios %s %s (%s-%s): %s",
                asset.epic,
                timeframe.name,
                current,
                batch_end,
                exc,
            )
            raise
        prices = payload.get("prices", []) if isinstance(payload, dict) else []
        if not prices:
            current = batch_end + timeframe.delta
            continue
        rows = rows_from_prices(asset, timeframe, prices)
        rows = [row for row in rows if effective_start <= row["fecha"] <= effective_end]
        if rows:
            if timeframe.table == DEFAULT_DAILY_TABLE:
                conflict_cols = ("asset_id", "fecha")
            else:
                conflict_cols = ("asset_id", "fecha", "intervalo")
            inserted = insert_rows(conn, timeframe.table, rows, conflict_cols)
            total_inserted += inserted
            logging.info(
                "Insertadas %s filas %s %s (%s - %s)",
                inserted,
                asset.epic,
                timeframe.intervalo,
                rows[0]["fecha"],
                rows[-1]["fecha"],
            )
        last_price_time = rows[-1]["fecha"] if rows else None
        if last_price_time:
            current = last_price_time + timeframe.delta
        else:
            current = batch_end + timeframe.delta
    return total_inserted


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Descarga históricos de CFDs desde Capital.com")
    parser.add_argument("--asset-id", type=int, action="append", help="Filtra por asset_id específico")
    parser.add_argument("--epic", action="append", help="Filtra por epic específico")
    parser.add_argument(
        "--start-date",
        type=lambda v: parse_datetime(v, "start-date"),
        default=parse_datetime(DEFAULT_START_DATE, "CAPITAL_START_DATE"),
        help="Fecha mínima (UTC) para cualquier descarga (ISO 8601)",
    )
    parser.add_argument(
        "--intraday-start-date",
        type=lambda v: parse_datetime(v, "intraday-start-date"),
        default=parse_datetime(DEFAULT_INTRADAY_START, "CAPITAL_INTRADAY_START"),
        help="Fecha mínima (UTC) para timeframes intradía",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("CAPITAL_LOG_LEVEL", "INFO"),
        help="Nivel de log (DEBUG, INFO, WARNING, ERROR)",
    )
    args = parser.parse_args(argv)

    configure_logging(args.log_level)

    api_key = os.environ.get("CAPITAL_API_KEY", "").strip()
    email = os.environ.get("CAPITAL_EMAIL", "").strip()
    password = os.environ.get("CAPITAL_PASSWORD", "").strip()

    db_params = {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "financial_data_v2"),
        "user": os.environ.get("PGUSER", ""),
        "password": os.environ.get("PGPASSWORD", ""),
    }

    client = CapitalAPIClient(api_key=api_key, identifier=email, password=password)

    try:
        conn = psycopg2.connect(**db_params)
    except psycopg2.Error as exc:
        logging.error("No se pudo conectar a PostgreSQL: %s", exc)
        return 1

    conn.autocommit = False

    try:
        assets = load_assets(conn, args.asset_id, args.epic)
        if not assets:
            logging.warning("No se encontraron assets con epic definido")
            return 0
        now_utc = dt.datetime.now(UTC).replace(microsecond=0)
        total_rows = 0
        for asset in assets:
            fine_grain_enabled = allows_fine_grain(asset)
            for timeframe in TIMEFRAMES:
                if timeframe.resolution in {"MINUTE", "MINUTE_5"} and not fine_grain_enabled:
                    logging.debug(
                        "Omitiendo %s para asset %s (%s)",
                        timeframe.name,
                        asset.asset_id,
                        asset.symbol or asset.epic,
                    )
                    continue
                start_date = args.start_date if timeframe.table == DEFAULT_DAILY_TABLE else args.intraday_start_date
                try:
                    inserted = fetch_and_store(conn, client, asset, timeframe, start_date, now_utc)
                except Exception:
                    conn.rollback()
                    logging.exception(
                        "Error procesando asset %s (%s) timeframe %s",
                        asset.asset_id,
                        asset.epic,
                        timeframe.name,
                    )
                    return 2
                else:
                    total_rows += inserted
        logging.info("Proceso completado. Filas insertadas/actualizadas: %s", total_rows)
        return 0
    finally:
        conn.close()
        client.session.close()


if __name__ == "__main__":
    sys.exit(main())
