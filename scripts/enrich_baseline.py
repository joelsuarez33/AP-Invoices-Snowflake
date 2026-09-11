"""
Enriquece el extracto FBL1N sintético existente:
  - multimoneda (WAERS ARS/USD/EUR, HWAER=ARS) con serie FX diaria
  - amplia cardinalidad de acreedores y clases de documento
  - re-ancla VERZN a la fecha de corte
  - emite seeds consistentes
Reproducible: SEED fijo.
"""
from __future__ import annotations
import json, datetime as dt
from pathlib import Path
import numpy as np, pandas as pd

SEED = 20260906
CUTOFF = pd.Timestamp("2026-09-06")
SRC = Path("/mnt/user-data/uploads/cc_sintetico.xlsx")
OUT = Path("/home/claude/work/out"); OUT.mkdir(exist_ok=True)
rng = np.random.default_rng(SEED)

df = pd.read_excel(SRC)
detail, total_row = df.iloc[:-1].copy(), df.iloc[-1:].copy()
assert len(detail) == 5000

# ---------- desplazamiento temporal: el ledger debe cerrar a la fecha de corte ----------
SHIFT = pd.Timedelta(days=218)
for c in ["Fecha de documento","Fe.contabilización","Vencimiento neto","Fecha compensación"]:
    detail[c] = detail[c] + SHIFT
# compensaciones que caerian en el futuro -> la partida todavia esta abierta al corte
future = detail["Fecha compensación"] > CUTOFF
detail.loc[future, ["Fecha compensación","Doc.compensación"]] = pd.NA

# ---------- serie FX sintetica (random walk con drift) ----------
dates = pd.date_range("2026-03-01", "2027-06-30", freq="D")
n = len(dates)
usd = np.empty(n); usd[0] = 1487.0
drift = (2010/1487) ** (1/n) - 1
for i in range(1, n):
    usd[i] = usd[i-1] * (1 + drift + rng.normal(0, 0.0035))
eur_ratio = 1.10 + np.cumsum(rng.normal(0, 0.0018, n)).clip(-0.06, 0.09)
fx = pd.DataFrame({
    "rate_date": dates,
    "ARS": 1.0,
    "USD": np.round(usd, 4),
    "EUR": np.round(usd * eur_ratio, 4),
})
fx_lookup = fx.set_index("rate_date")

# ---------- acreedores ----------
ORIG = [2505, 4040, 4650, 6930, 9059]
pool = ORIG + sorted(rng.choice([v for v in range(1000, 9999) if v not in ORIG], 35, replace=False).tolist())
# distribucion Pareto-ish: pocos acreedores concentran el volumen
w = rng.pareto(1.6, len(pool)) + 0.3; w /= w.sum()
detail["Acreedor"] = rng.choice(pool, size=len(detail), p=w)

# ---------- clase de documento, coherente con el signo existente ----------
amt = detail["Importe en moneda local"].to_numpy(float)
blart = np.empty(len(detail), dtype=object)
neg, pos = amt < 0, amt >= 0
blart[neg] = rng.choice(["KR", "RE"], neg.sum(), p=[0.85, 0.15])   # credito al acreedor (SHKZG=H)
blart[pos] = rng.choice(["KG", "KZ"], pos.sum(), p=[0.40, 0.60])   # debito (SHKZG=S)
detail["Clase de documento"] = blart

# ---------- multimoneda ----------
# DMBTR (local, ARS) se preserva -> el saldo total del archivo no cambia.
# WRBTR se deriva dividiendo por el tipo de cambio a BLDAT.
waers = rng.choice(["ARS", "USD", "EUR"], len(detail), p=[0.70, 0.20, 0.10])
detail["Moneda del documento"] = waers
detail["Moneda local"] = "ARS"
rates = np.array([fx_lookup.loc[d, c] for d, c in zip(detail["Fecha de documento"], waers)])
detail["Importe en moneda doc."] = np.round(amt / rates, 2)
detail["Importe en moneda local"] = np.round(amt, 2)

# ---------- VERZN re-anclado a la fecha de corte ----------
is_open = detail["Fecha compensación"].isna()
verzn = (CUTOFF - detail["Vencimiento neto"]).dt.days
detail["Demora tras vencimiento neto"] = verzn.where(is_open, other=pd.NA).astype("Int64")

# ---------- fila de totales ----------
# SAP no suma importes en moneda de documento cuando hay monedas mezcladas: muestra '*'
total_row = total_row.copy()
total_row["Importe en moneda doc."] = "*"
total_row["Importe en moneda local"] = round(float(detail["Importe en moneda local"].sum()), 2)

out = pd.concat([detail, total_row], ignore_index=True)
xlsx = OUT / "FBL1N_20260906.xlsx"
out.to_excel(xlsx, index=False, sheet_name="Sheet1")

# ---------- seeds ----------
paises = ["AR"]*24 + ["BR"]*6 + ["DE"]*4 + ["US"]*4 + ["IT"]*2
ztermk = ["Z030","Z045","Z060","Z015"]
vendors = pd.DataFrame({
    "lifnr": pool,
    "vendor_name": [f"PROVEEDOR {i:03d} SA" for i in range(1, len(pool)+1)],
    "country": rng.permutation(paises),
    "payment_terms": rng.choice(ztermk, len(pool), p=[0.45,0.30,0.15,0.10]),
    "purchasing_group": rng.choice(["P01","P02","P03","P04"], len(pool)),
})
vendors.to_csv(OUT/"vendors.csv", index=False)

fx_long = fx.melt("rate_date", var_name="currency", value_name="rate_to_ars")
fx_long["rate_date"] = fx_long["rate_date"].dt.strftime("%Y-%m-%d")
fx_long.to_csv(OUT/"fx_rates.csv", index=False)

pd.DataFrame({
    "blart": ["KR","RE","KG","KZ"],
    "description": ["Factura de acreedor","Factura logistica (MM)","Nota de credito","Pago saliente"],
    "debit_credit_indicator": ["H","H","S","S"],
    "is_invoice": [True,True,False,False],
}).to_csv(OUT/"document_types.csv", index=False)

# ---------- state para que el generador continue ----------
state = {
    "seed": SEED,
    "last_run_date": CUTOFF.strftime("%Y-%m-%d"),
    "last_belnr": int(detail["Nº documento"].max()),
    "last_kidno": int(detail["Clave de referencia"].max()),
    "last_augbl": int(detail["Doc.compensación"].max()),
    "open_items": int(is_open.sum()),
    "total_items": int(len(detail)),
    "vendors": [int(v) for v in pool],
}
(OUT/"_state.json").write_text(json.dumps(state, indent=2))

print("revertidas a abierta por corte:", int(future.sum()))
print("filas detalle:", len(detail))
print("total local ARS:", f'{detail["Importe en moneda local"].sum():,.2f}')
print("abiertas:", int(is_open.sum()), "| compensadas:", int((~is_open).sum()))
print("VERZN rango:", int(verzn[is_open].min()), "a", int(verzn[is_open].max()))
print("mix moneda:", detail["Moneda del documento"].value_counts().to_dict())
print("mix BLART:", detail["Clase de documento"].value_counts().to_dict())
print("acreedores:", detail["Acreedor"].nunique(), "| top5 share:",
      round(detail["Acreedor"].value_counts(normalize=True).head(5).sum()*100,1), "%")
print("BLDAT:", detail["Fecha de documento"].min().date(), "->", detail["Fecha de documento"].max().date())
print("NETDT:", detail["Vencimiento neto"].min().date(), "->", detail["Vencimiento neto"].max().date())
import numpy as _np
_b=pd.cut(verzn[is_open],[-999,-1,30,60,90,9999],labels=["no vencida","1-30","31-60","61-90","90+"])
print("aging abiertas:", _b.value_counts().reindex(["no vencida","1-30","31-60","61-90","90+"]).to_dict())
print("FX USD:", round(fx.USD.iloc[0],2), "->", round(fx.USD.iloc[-1],2))
