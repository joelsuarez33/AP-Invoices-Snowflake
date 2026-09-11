"""Diagnostico de la autenticacion key-pair contra Snowflake.

Solo lee: no modifica .env, ni load.py, ni nada en Snowflake. Imprime el estado
de cada variable, la huella derivada de la clave privada, el modo de
autenticacion que resolveria load.py, la deriva del reloj local y el resultado
de tres intentos de conexion con variantes acotadas.

Regla de salida: ninguna clave, passphrase o token llega a stdout. De las
variables sensibles solo se reporta si estan definidas, si estan vacias y su
longitud.

    python scripts/diagnose_auth.py
"""

from __future__ import annotations

import base64
import hashlib
import os
import sys
import time
import traceback
import urllib.request
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# Importar config es lo que carga el .env, exactamente como hace load.py.
import config  # noqa: E402
import load  # noqa: E402

# Identificadores, no credenciales: el locator de la cuenta y el umbral de
# deriva a partir del cual el iat del JWT empieza a caer fuera de ventana.
ACCOUNT_LOCATOR = "DK49477"
CLOCK_SKEW_LIMIT_SECONDS = 30.0
NTP_HOSTS = ("time.windows.com", "pool.ntp.org", "time.google.com")
NTP_UNIX_DELTA = 2208988800

SENSITIVE_MARKERS = ("PASSPHRASE", "PASSWORD", "SECRET", "TOKEN")


def is_sensitive(name: str) -> bool:
    upper = name.upper()
    if "PRIVATE_KEY" in upper and "PATH" not in upper:
        return True
    return any(marker in upper for marker in SENSITIVE_MARKERS)


def section(title: str) -> None:
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


# ---------------------------------------------------------------------------
# 1. Variables de entorno
# ---------------------------------------------------------------------------
def report_env() -> None:
    section("1. Variables SNOWFLAKE_* visibles para el proceso (post load_dotenv)")

    declared: set[str] = set()
    duplicated: list[str] = []
    env_file = REPO_ROOT / ".env"
    if env_file.exists():
        seen: list[str] = []
        for line in env_file.read_text(encoding="utf-8-sig").splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                key = stripped.split("=", 1)[0].strip()
                if key in seen:
                    duplicated.append(key)
                seen.append(key)
                declared.add(key)

    names = sorted({k for k in os.environ if k.upper().startswith("SNOWFLAKE")} | declared)
    if not names:
        print("  (no hay ninguna)")
        return

    for name in names:
        if name not in os.environ:
            print(f"  {name:36} NO DEFINIDA")
            continue
        raw = os.environ[name]
        state = "VACIA    " if raw.strip() == "" else "con valor"
        line = f"  {name:36} definida | {state} | len={len(raw)}"
        if is_sensitive(name):
            line += " | <sensible: valor omitido>"
        else:
            line += f" | head={raw[:8]!r}"
        if raw != raw.strip():
            line += " | OJO: espacios alrededor"
        if raw[:1] in ("'", '"') or raw[-1:] in ("'", '"'):
            line += " | OJO: comillas sin desarmar"
        print(line)

    if duplicated:
        print()
        print(f"  Claves repetidas dentro del .env (gana la ultima): {sorted(set(duplicated))}")

    print()
    print("  Como las lee config.optional_env (.strip() + truthiness):")
    for name in (
        "SNOWFLAKE_PRIVATE_KEY_PATH",
        "SNOWFLAKE_PRIVATE_KEY",
        "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
    ):
        value = config.optional_env(name)
        verdict = "<vacio/falsy>" if not value else "truthy"
        print(f"    optional_env({name:34}) -> {verdict} (len={len(value)})")


# ---------------------------------------------------------------------------
# 2. Modo de autenticacion que resolveria load.py
# ---------------------------------------------------------------------------
def report_auth_mode() -> None:
    section("2. Modo de autenticacion que resuelve load.load_private_key()")

    key_path = config.optional_env("SNOWFLAKE_PRIVATE_KEY_PATH")
    key_pem = config.optional_env("SNOWFLAKE_PRIVATE_KEY")
    passphrase = config.optional_env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

    print(f"  SNOWFLAKE_PRIVATE_KEY_PATH truthy tras strip : {bool(key_path)}")
    print(f"  SNOWFLAKE_PRIVATE_KEY      truthy tras strip : {bool(key_pem)}")
    passphrase_arg = "None" if not passphrase else "<bytes de la passphrase>"
    print(f"  password= que recibe load_pem_private_key    : {passphrase_arg}")

    if key_path and key_pem:
        print("  MODO: CONFLICTO -> load.py levanta ConfigError antes de conectar")
    elif key_path:
        print(f"  MODO: LOCAL (archivo en disco) -> {key_path!r}")
        path = Path(key_path).expanduser()
        print(f"    existe          : {path.exists()}")
        if path.exists():
            print(f"    resuelve a      : {path.resolve()}")
            first = path.read_bytes().splitlines()[0].decode("ascii", "replace")
            print(f"    primera linea   : {first}")
            print(f"    cifrada         : {'ENCRYPTED' in first}")
    elif key_pem:
        print("  MODO: CI (PEM completo en variable de entorno)")
    else:
        print("  MODO: NINGUNO -> load.py levanta ConfigError")

    print()
    print("  config.optional_env hace os.environ.get(name, '').strip() or default: decide")
    print("  por truthiness, no por presencia. Una variable presente pero vacia no cambia")
    print("  de modo, y la passphrase vacia llega como None, no como cadena vacia.")


# ---------------------------------------------------------------------------
# 3. Huella de la clave publica derivada de la privada
# ---------------------------------------------------------------------------
def report_fingerprint() -> str | None:
    section("3. Huella SHA256 derivada de la clave privada que lee load.py")
    try:
        from cryptography.hazmat.primitives import serialization

        der = load.load_private_key()
        private_key = serialization.load_der_private_key(der, password=None)
        public_der = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        digest = base64.b64encode(hashlib.sha256(public_der).digest()).decode("ascii")
        fingerprint = f"SHA256:{digest}"
        print(f"  {fingerprint}")
        print("  Comparar contra RSA_PUBLIC_KEY_FP de DESCRIBE USER en Snowflake.")
        return fingerprint
    except Exception:
        print("  FALLO al derivar la huella:")
        traceback.print_exc(file=sys.stdout)
        return None


# ---------------------------------------------------------------------------
# 4. Deriva del reloj
# ---------------------------------------------------------------------------
def ntp_offset(host: str) -> tuple[float, float]:
    """Offset (local - referencia) y RTT contra un servidor NTP, en segundos."""
    import socket
    import struct

    packet = b"\x1b" + 47 * b"\0"
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(5)
    try:
        t1 = time.time()
        sock.sendto(packet, (host, 123))
        data, _ = sock.recvfrom(48)
        t4 = time.time()
    finally:
        sock.close()
    fields = struct.unpack("!12I", data)
    t2 = fields[8] + float(fields[9]) / 2**32 - NTP_UNIX_DELTA
    t3 = fields[10] + float(fields[11]) / 2**32 - NTP_UNIX_DELTA
    return -(((t2 - t1) + (t3 - t4)) / 2), t4 - t1


def report_clock_skew() -> None:
    section("4. Deriva del reloj local (el JWT lleva iat y Snowflake valida la ventana)")

    # Medicion autoritativa: SNTP da precision de milisegundos contra el mismo
    # patron de tiempo que usa el servidor. Va primero porque es la que decide.
    print("  4a. Contra NTP (autoritativo, precision de milisegundos)")
    measured = False
    for host in NTP_HOSTS:
        try:
            offset, rtt = ntp_offset(host)
            measured = True
            verdict = "SOSPECHOSO" if abs(offset) > CLOCK_SKEW_LIMIT_SECONDS else "OK"
            print(f"    {host:20} local - NTP = {offset:+8.3f} s  (rtt={rtt:.3f}s)  {verdict}")
        except Exception as exc:
            print(f"    {host:20} ERROR: {type(exc).__name__}: {exc}")
    if measured:
        print(f"    Umbral: +-{CLOCK_SKEW_LIMIT_SECONDS:.0f} s. Si se supera, Snowflake rechaza")
        print("    el JWT con ERROR_CODE 390144 / JWT_TOKEN_INVALID_ISSUE_TIME.")

    # Medicion secundaria, deliberadamente NO decisiva: el header Date de
    # api.github.com sale de cache de CDN y en pruebas dio saltos de hasta 20 s
    # con el reloj local sano. Sirve como sanity check grueso, nada mas.
    print()
    print("  4b. Contra el header Date de api.github.com (referencia gruesa, ruidosa)")
    try:
        request = urllib.request.Request("https://api.github.com", method="HEAD")
        request.add_header("User-Agent", "diagnose-auth")
        before = datetime.now(UTC)
        with urllib.request.urlopen(request, timeout=15) as response:
            after = datetime.now(UTC)
            date_header = response.headers.get("Date")
        if not date_header:
            print("    La respuesta no trajo header Date; no se puede medir.")
            return
        remote = parsedate_to_datetime(date_header)
        local = before + (after - before) / 2
        drift = (local - remote).total_seconds()
        print(f"    Date remoto : {remote.isoformat()}")
        print(f"    Reloj local : {local.isoformat()}")
        print(f"    local - Date: {drift:+.1f} s   (rtt={(after - before).total_seconds():.3f}s)")
        if abs(drift) > CLOCK_SKEW_LIMIT_SECONDS:
            print(f"    Supera {CLOCK_SKEW_LIMIT_SECONDS:.0f} s, pero NO concluir de aca:")
            print("    confirmar siempre contra 4a antes de tocar el reloj.")
    except Exception:
        print("    FALLO al medir contra api.github.com:")
        traceback.print_exc(file=sys.stdout)


# ---------------------------------------------------------------------------
# 5. Parametros que load.connect() le pasa al conector
# ---------------------------------------------------------------------------
def report_connect_params() -> None:
    section("5. Parametros que load.connect() le pasa a snowflake.connector.connect()")
    account = config.optional_env("SNOWFLAKE_ACCOUNT")
    print(f"  account   = {account!r}   (load.py lo pasa tal cual, sin upper ni split)")
    print(f"  user      = {config.optional_env('SNOWFLAKE_USER')!r}")
    print(f"  role      = {config.optional_env('SNOWFLAKE_ROLE')!r}")
    print(f"  warehouse = {config.optional_env('SNOWFLAKE_WAREHOUSE')!r}")
    try:
        objects = config.snowflake_objects()
        print(f"  database  = {objects['database']!r}")
        print(f"  schema    = {objects['schema_raw']!r}")
    except config.ConfigError as exc:
        print(f"  objetos   = NO RESUELTOS: {exc}")
    print("  private_key = <DER PKCS8, omitido>")
    print("  client_session_keep_alive = False")
    print("  application = 'ap_elt_pipeline'")
    print("  authenticator / password / token / session_parameters: no se pasan")
    print()
    print(f"  Host que arma el conector: {account.lower()}.snowflakecomputing.com:443")


# ---------------------------------------------------------------------------
# 6. Intentos de conexion
# ---------------------------------------------------------------------------
def attempt(label: str, account: str, private_key: bytes) -> bool:
    print()
    print(f"  --- Intento: {label}")
    print(f"      account = {account!r}")
    try:
        import snowflake.connector

        connection = snowflake.connector.connect(
            account=account,
            user=config.require_env("SNOWFLAKE_USER"),
            role=config.require_env("SNOWFLAKE_ROLE"),
            warehouse=config.require_env("SNOWFLAKE_WAREHOUSE"),
            private_key=private_key,
            client_session_keep_alive=False,
            application="ap_elt_diagnose",
            login_timeout=30,
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "select current_account(), current_user(), current_role(), current_version()"
                )
                row = cursor.fetchone()
            print(f"      OK -> account={row[0]} user={row[1]} role={row[2]} version={row[3]}")
        finally:
            connection.close()
        return True
    except Exception:
        print("      FALLO:")
        for line in traceback.format_exc().splitlines():
            print(f"      {line}")
        return False


def load_key_with_passphrase_forced_none() -> bytes:
    """Reconstruye el DER ignorando por completo SNOWFLAKE_PRIVATE_KEY_PASSPHRASE."""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    key_path = config.optional_env("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        material = Path(key_path).expanduser().read_bytes()
    else:
        material = config.optional_env("SNOWFLAKE_PRIVATE_KEY").replace("\\n", "\n").encode()
    private_key = serialization.load_pem_private_key(
        material, password=None, backend=default_backend()
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def report_attempts() -> None:
    section("6. Intentos de conexion")
    results: dict[str, bool] = {}
    env_account = config.optional_env("SNOWFLAKE_ACCOUNT")

    try:
        der = load.load_private_key()
    except Exception:
        print("  No se pudo cargar la clave por la ruta normal de load.py:")
        traceback.print_exc(file=sys.stdout)
        der = None

    if der is not None:
        results["a) account del .env, clave como la arma load.py"] = attempt(
            "account del .env, clave como la arma load.py", env_account, der
        )
        results[f"b) account locator {ACCOUNT_LOCATOR}"] = attempt(
            f"account locator {ACCOUNT_LOCATOR}", ACCOUNT_LOCATOR, der
        )

    try:
        der_none = load_key_with_passphrase_forced_none()
        results["c) account del .env, passphrase forzada a None"] = attempt(
            "account del .env, passphrase forzada a None", env_account, der_none
        )
    except Exception:
        print()
        print("  --- Intento: passphrase forzada a None")
        print("      No se pudo ni cargar la clave:")
        traceback.print_exc(file=sys.stdout)
        results["c) account del .env, passphrase forzada a None"] = False

    print()
    print("  Resumen de intentos:")
    for label, ok in results.items():
        print(f"    {'OK    ' if ok else 'FALLO '} {label}")


# ---------------------------------------------------------------------------
# 7. Historial de logins segun Snowflake
# ---------------------------------------------------------------------------
LOGIN_HISTORY_SQL = """
select to_char(EVENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') as TS,
       REPORTED_CLIENT_VERSION as DRIVER,
       FIRST_AUTHENTICATION_FACTOR as AUTH,
       IS_SUCCESS,
       ERROR_CODE,
       ERROR_MESSAGE
from table(information_schema.login_history(
    dateadd('day', -6, current_timestamp()), current_timestamp()))
order by EVENT_TIMESTAMP
"""


def report_login_history() -> None:
    """Lo que Snowflake registro del lado servidor.

    Es la unica fuente que distingue entre las causas posibles de un 250001:
    ERROR_CODE 390144 / JWT_TOKEN_INVALID_ISSUE_TIME es deriva de reloj,
    390144 con otro mensaje o 390318 apuntan a la clave o al issuer. El cliente
    colapsa todos esos casos en el mismo 'JWT token is invalid'.
    """
    section("7. Historial de logins segun Snowflake (ultimos 6 dias)")
    try:
        connection = load.connect()
    except Exception:
        print("  No hay conexion disponible para consultarlo:")
        traceback.print_exc(file=sys.stdout)
        return
    try:
        with connection.cursor() as cursor:
            cursor.execute(LOGIN_HISTORY_SQL)
            rows = cursor.fetchall()
        if not rows:
            print("  (sin intentos registrados en la ventana)")
            return
        print(f"  {'TIMESTAMP (TZ de la cuenta)':28} {'DRIVER':10} {'OK':4} {'CODE':8} MENSAJE")
        for ts, driver, _auth, ok, code, message in rows:
            print(f"  {ts:28} {driver or '-':10} {ok:4} {code or '-'!s:8} {message or ''}")
        failures = [r for r in rows if r[3] != "YES"]
        print()
        print(f"  Intentos: {len(rows)} | fallidos: {len(failures)}")
        if any(r[5] == "JWT_TOKEN_INVALID_ISSUE_TIME" for r in failures):
            print("  Hay JWT_TOKEN_INVALID_ISSUE_TIME: el iat quedo fuera de ventana.")
            print("  Es deriva de reloj en el host, no un problema de clave ni de account.")
    except Exception:
        print("  FALLO al consultar el historial:")
        traceback.print_exc(file=sys.stdout)
    finally:
        connection.close()


def main() -> int:
    print("Diagnostico de autenticacion key-pair - Snowflake")
    print(f"repo   : {REPO_ROOT}")
    print(f"python : {sys.version.split()[0]}")
    try:
        import snowflake.connector

        print(f"connector: {snowflake.connector.__version__}")
    except Exception as exc:
        print(f"connector: no importable ({exc})")

    report_env()
    report_auth_mode()
    report_fingerprint()
    report_clock_skew()
    report_connect_params()
    report_attempts()
    report_login_history()
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
