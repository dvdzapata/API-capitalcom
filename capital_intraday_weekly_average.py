"""Genera promedios intradía por día de la semana para el índice US100.

El script se autentica contra la API de Capital.com usando credenciales
almacenadas en un archivo `.env`. Para cada día de la semana obtiene las
últimas 50 sesiones (00:00 a 00:00 UTC del día siguiente), calcula la media
minuto a minuto y genera un gráfico con el promedio intradía de cada día.

Requisitos:
    pip install pandas requests matplotlib python-dotenv (opcional)
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

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
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    df["minute"] = (df["timestamp"] - session_start).dt.total_seconds() / 60.0
    df = df[(df["minute"] >= 0) & (df["minute"] <= 24 * 60)]
    df["minute"] = df["minute"].round().astype(int)
    df = df.groupby("minute", as_index=False)["close"].mean()
    return df


def compute_average_intraday(
    client: CapitalClient,
    epic: str,
    weekday: int,
    sessions: int = 50,
    resolution: str = "MINUTE",
) -> pd.DataFrame:
    session_starts = daterange_for_weekday(weekday, sessions)
    if not session_starts:
        raise CapitalAPIError(f"No se encontraron fechas para el weekday {weekday}")

    frames: List[pd.DataFrame] = []
    for start in session_starts:
        df = build_intraday_dataframe(client, epic, start, resolution=resolution)
        if df is None:
            continue
        frames.append(df)
    if not frames:
        raise CapitalAPIError(f"No se obtuvieron datos válidos para weekday {weekday}")

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
    profiles: Dict[int, pd.DataFrame] = {}
    for weekday in range(7):
        try:
            logger.info("Procesando día %s", weekday)
            profile = compute_average_intraday(client, epic, weekday)
            valid_points = profile["sessions_used"]
            coverage = (valid_points >= 1).sum()
            logger.info(
                "Día %s: puntos con datos %s/%s", weekday, coverage, profile.shape[0]
            )
            profiles[weekday] = profile
        except CapitalAPIError as exc:
            logger.warning("No se pudo calcular el día %s: %s", weekday, exc)
            profiles[weekday] = pd.DataFrame(columns=["minute", "average_close", "time", "sessions_used"])
    output_path = Path("outputs/us100_intradia_promedios.png")
    plot_weekday_profiles(profiles, output_path)
    logger.info("Gráfico generado en %s", output_path.resolve())


if __name__ == "__main__":
    try:
        main()
    except CapitalAPIError as exc:
        logging.getLogger("capital_intradia").error("Ejecución detenida: %s", exc)
        raise
