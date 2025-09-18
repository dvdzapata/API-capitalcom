# Ejecutar pipeline completo: autenticación, fetch de mercados AVGO/AMD/NVDA, normalización e inserción en ClickHouse.
# Incluye manejo básico de duplicados mediante tablas ReplacingMergeTree y deduplicación por (epic, as_of) con versión.

import os
import json
import datetime as dt
import requests
import pandas as pd
from tqdm import tqdm
from clickhouse_connect import get_client

CAPITAL_API_BASE = 'https://api-capital.backend-capital.com/api/v1'
CAPITAL_API_KEY = os.environ.get('CAPITAL_API_KEY', '').strip()
CAPITAL_EMAIL = os.environ.get('CAPITAL_EMAIL', '').strip()
CAPITAL_PASSWORD = os.environ.get('CAPITAL_PASSWORD', '').strip()
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = 'default'

print('Inicio de ingesta')

# Sesion y login
session = requests.Session()
session.headers.update({'X-CAP-API-KEY': CAPITAL_API_KEY, 'Content-Type': 'application/json'})
login_resp = session.post(CAPITAL_API_BASE + '/session', data=json.dumps({'identifier': CAPITAL_EMAIL, 'password': CAPITAL_PASSWORD}))
if login_resp.status_code not in [200, 201]:
    raise RuntimeError('Login fallo status ' + str(login_resp.status_code) + ' body ' + login_resp.text)

cst = login_resp.headers.get('CST')
xst = login_resp.headers.get('X-SECURITY-TOKEN')
if not cst or not xst:
    body = {}
    try:
        body = login_resp.json()
    except Exception:
        body = {}
    cst = cst or body.get('CST')
    xst = xst or body.get('securityToken') or body.get('X-SECURITY-TOKEN')
if not cst or not xst:
    raise RuntimeError('Tokens no recibidos')

session.headers.update({'CST': cst, 'X-SECURITY-TOKEN': xst})
print('Autenticado OK')

# Descubrir epics
symbols = ['AVGO', 'AMD', 'NVDA']
epics = []
for sym in symbols:
    r = session.get(CAPITAL_API_BASE + '/markets?searchTerm=' + sym)
    if r.status_code != 200:
        print('Aviso busqueda ' + sym + ' status ' + str(r.status_code))
        continue
    payload = r.json()
    for key in ['markets', 'marketDetails']:
        arr = payload.get(key, []) if isinstance(payload, dict) else []
        if isinstance(arr, dict):
            arr = [arr]
        for item in arr:
            epic = None
            if isinstance(item, dict):
                if 'epic' in item:
                    epic = item.get('epic')
                elif 'instrument' in item and isinstance(item['instrument'], dict):
                    epic = item['instrument'].get('epic')
            if epic and epic not in epics:
                epics.append(epic)

print('Epics detectados: ' + ', '.join(epics))

# Obtener detalles por epic
records = []
as_of = dt.datetime.utcnow().replace(microsecond=0)
for epic in tqdm(epics):
    r = session.get(CAPITAL_API_BASE + '/markets/' + str(epic))
    if r.status_code != 200:
        print('Fallo detalles ' + str(epic) + ' status ' + str(r.status_code))
        continue
    payload = r.json()
    records.append({'epic': epic, 'payload': payload, 'as_of': as_of})

print('Detalles descargados: ' + str(len(records)))

# Normalización a DataFrames
meta_rows = []
rules_rows = []
hours_rows = []
snapshot_rows = []

for rec in records:
    epic = rec['epic']
    as_of_ts = rec['as_of']
    p = rec['payload']

    inst = p.get('instrument', {}) if isinstance(p, dict) else {}
    meta_rows.append({
        'as_of': as_of_ts,
        'epic': epic,
        'symbol': inst.get('symbol'),
        'name': inst.get('name'),
        'type': inst.get('type') or inst.get('instrumentType'),
        'currency': inst.get('currency'),
        'lot_size': pd.to_numeric(inst.get('lotSize'), errors='coerce'),
        'guaranteed_stop_allowed': 1 if inst.get('guaranteedStopAllowed') else 0,
        'streaming_prices_available': 1 if inst.get('streamingPricesAvailable') else 0,
        'margin_factor': pd.to_numeric(inst.get('marginFactor'), errors='coerce'),
        'margin_factor_unit': inst.get('marginFactorUnit')
    })

    opening = inst.get('openingHours', {}) if isinstance(inst, dict) else {}
    if isinstance(opening, dict):
        for day, segments in opening.items():
            seg_json = None
            try:
                seg_json = json.dumps(segments, ensure_ascii=False)
            except Exception:
                seg_json = None
            hours_rows.append({'as_of': as_of_ts, 'epic': epic, 'day': day, 'segments_json': seg_json})

    rules = p.get('dealingRules', {}) if isinstance(p, dict) else {}
    if isinstance(rules, dict):
        def pick_num_unit(obj):
            if isinstance(obj, dict):
                return obj.get('value'), obj.get('unit')
            return None, None
        ms_val, ms_unit = pick_num_unit(rules.get('minStep'))
        mins_val, mins_unit = pick_num_unit(rules.get('minSize'))
        maxs_val, maxs_unit = pick_num_unit(rules.get('maxSize'))
        msd_val, msd_unit = pick_num_unit(rules.get('minStopDistance'))
        mld_val, mld_unit = pick_num_unit(rules.get('minLimitDistance'))
        rules_rows.append({
            'as_of': as_of_ts,
            'epic': epic,
            'min_step_value': pd.to_numeric(ms_val, errors='coerce'),
            'min_step_unit': ms_unit or '',
            'min_size_value': pd.to_numeric(mins_val, errors='coerce'),
            'min_size_unit': mins_unit or '',
            'max_size_value': pd.to_numeric(maxs_val, errors='coerce'),
            'max_size_unit': maxs_unit or '',
            'min_stop_distance_value': pd.to_numeric(msd_val, errors='coerce'),
            'min_stop_distance_unit': msd_unit or '',
            'min_limit_distance_value': pd.to_numeric(mld_val, errors='coerce'),
            'min_limit_distance_unit': mld_unit or '',
            'trailing_stops_allowed': 1 if rules.get('trailingStopsAllowed') else 0,
            'guaranteed_stops_allowed': 1 if rules.get('guaranteedStopsAllowed') else 0
        })

    snap = p.get('snapshot') if isinstance(p, dict) else None
    if isinstance(snap, dict):
        snapshot_rows.append({
            'as_of': as_of_ts,
            'epic': epic,
            'tradingStatus': str(snap.get('tradingStatus')) if 'tradingStatus' in snap else '',
            'marketStatus': str(snap.get('marketStatus')) if 'marketStatus' in snap else '',
            'scalingFactor': int(pd.to_numeric(snap.get('scalingFactor'), errors='coerce')) if snap.get('scalingFactor') is not None else 0,
            'market24h': 1 if snap.get('market24h') else 0
        })

meta_df = pd.DataFrame(meta_rows)
rules_df = pd.DataFrame(rules_rows)
hours_df = pd.DataFrame(hours_rows)
snapshot_df = pd.DataFrame(snapshot_rows)

print('meta_df head:')
print(meta_df.head())
print('rules_df head:')
print(rules_df.head())
print('hours_df head:')
print(hours_df.head())
print('snapshot_df head:')
print(snapshot_df.head())

# ClickHouse cliente
client = get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)
print('CH conectado')

# Crear tablas con ReplacingMergeTree por as_of para dedup por última versión
client.command('''
CREATE TABLE IF NOT EXISTS cfd_instrument_meta (
  as_of DateTime,
  epic String,
  symbol String,
  name String,
  type String,
  currency String,
  lot_size Float64,
  guaranteed_stop_allowed UInt8,
  streaming_prices_available UInt8,
  margin_factor Float64,
  margin_factor_unit String
) ENGINE = ReplacingMergeTree(as_of)
ORDER BY (epic, as_of)
''')

client.command('''
CREATE TABLE IF NOT EXISTS cfd_dealing_rules (
  as_of DateTime,
  epic String,
  min_step_value Float64,
  min_step_unit String,
  min_size_value Float64,
  min_size_unit String,
  max_size_value Float64,
  max_size_unit String,
  min_stop_distance_value Float64,
  min_stop_distance_unit String,
  min_limit_distance_value Float64,
  min_limit_distance_unit String,
  trailing_stops_allowed UInt8,
  guaranteed_stops_allowed UInt8
) ENGINE = ReplacingMergeTree(as_of)
ORDER BY (epic, as_of)
''')

client.command('''
CREATE TABLE IF NOT EXISTS cfd_opening_hours (
  as_of DateTime,
  epic String,
  day String,
  segments_json String
) ENGINE = ReplacingMergeTree(as_of)
ORDER BY (epic, day, as_of)
''')

client.command('''
CREATE TABLE IF NOT EXISTS cfd_snapshot (
  as_of DateTime,
  epic String,
  tradingStatus String,
  marketStatus String,
  scalingFactor Int32,
  market24h UInt8
) ENGINE = ReplacingMergeTree(as_of)
ORDER BY (epic, as_of)
''')

print('Tablas OK')

# Inserción
if not meta_df.empty:
    client.insert_df('cfd_instrument_meta', meta_df)
    print('Inserted meta rows: ' + str(len(meta_df)))
else:
    print('No meta rows')

if not rules_df.empty:
    client.insert_df('cfd_dealing_rules', rules_df)
    print('Inserted rules rows: ' + str(len(rules_df)))
else:
    print('No rules rows')

if not hours_df.empty:
    client.insert_df('cfd_opening_hours', hours_df)
    print('Inserted hours rows: ' + str(len(hours_df)))
else:
    print('No hours rows')

if not snapshot_df.empty:
    client.insert_df('cfd_snapshot', snapshot_df)
    print('Inserted snapshot rows: ' + str(len(snapshot_df)))
else:
    print('No snapshot rows')

# Conteo final
c1 = client.query('SELECT count() FROM cfd_instrument_meta')
c2 = client.query('SELECT count() FROM cfd_dealing_rules')
c3 = client.query('SELECT count() FROM cfd_opening_hours')
c4 = client.query('SELECT count() FROM cfd_snapshot')
print('cfd_instrument_meta total:', c1.result_rows[0][0])
print('cfd_dealing_rules total:', c2.result_rows[0][0])
print('cfd_opening_hours total:', c3.result_rows[0][0])
print('cfd_snapshot total:', c4.result_rows[0][0])

print('Ingesta finalizada')

-----------------------------------------------------------------------------------------------------

instrucciones

Paso 1 — Proveer credenciales Capital API

Exporta variables antes de ejecutar el pipeline:
En Linux/macOS:
export CAPITAL_API_KEY="TU_API_KEY"
export CAPITAL_EMAIL="tu_email_de_capital"
export CAPITAL_PASSWORD="tu_password"
En Windows PowerShell:
setx CAPITAL_API_KEY "TU_API_KEY"
setx CAPITAL_EMAIL "tu_email_de_capital"
setx CAPITAL_PASSWORD "tu_password"
Luego reinicia el kernel/notebook y vuelve a correr la ingesta.
Qué hice:

Instalé dependencias y corrí el pipeline; falló login por identifier nulo (no hay CAPITAL_EMAIL). Te dejo listo para reintentar con variables cargadas.
Paso 2 — Automatizar ejecución (cron en Linux)

Crea un script run_ingesta.sh:
source ~/.bashrc
cd /ruta/al/notebook
jupyter nbconvert --to notebook --execute pipeline_capital_clickhouse.ipynb --output pipeline_out.ipynb
Da permisos: chmod +x run_ingesta.sh
Cron diario 07:05 UTC:
crontab -e
5 7 * * * /ruta/run_ingesta.sh >> /ruta/ingesta.log 2>&1
Paso 3 — Healthcheck y alertas mínimas

Añade al final del pipeline una verificación:
SELECT count() FROM cfd_instrument_meta WHERE as_of >= now() - interval 1 day
Si 0, loggea “sin datos hoy” y exit code 1 (para alertarte con tu monitor).
Si prefieres, lo dejo como servicio systemd en vez de cron.


