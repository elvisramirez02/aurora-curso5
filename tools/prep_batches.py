#!/usr/bin/env python3
"""
prep_batches.py — Genera el repo de datos del Curso 5 (Aurora Pipeline).

Produce, de forma DETERMINISTA (semilla fija):
  dims/v1/*.csv, dims/v2/*.csv
  batches/b001 ... b006/fact_ventas.csv
  batches/b006_malo/fact_ventas.csv       (drill D1: formato de monto roto)
  batches/b007_corrupto/fact_ventas.csv   (drill D4: error semantico que pasa validaciones)
  expected.json                            (conteos y sumas esperados por lote y capa)

Dos modos de entrada:
  1) --synthetic (default)   genera un dataset sintetico con el schema placeholder.
  2) --mapping mapping.json  lee los CSV reales de Tiendas Aurora y los renombra al
                             schema canonico (ver README, seccion "Datos reales").

Solo libreria estandar de Python. Uso:
    python tools/prep_batches.py --out .                      # sintetico
    python tools/prep_batches.py --out . --rows 230000        # mas volumen
    python tools/prep_batches.py --out . --mapping map.json --src ./aurora_csv
"""
import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import random
import re
from collections import Counter

DIMS = ["dim_tienda", "dim_producto", "dim_cliente", "dim_empleado"]
DIM_KEYS = {"dim_tienda": "tienda_id", "dim_producto": "producto_id",
            "dim_cliente": "cliente_id", "dim_empleado": "empleado_id"}
DIM_COLS = {
    "dim_tienda": ["tienda_id", "nombre", "ciudad", "formato"],
    "dim_producto": ["producto_id", "nombre", "categoria", "costo_unitario", "precio_lista"],
    "dim_cliente": ["cliente_id", "nombre", "segmento"],
    "dim_empleado": ["empleado_id", "nombre", "tienda_id", "cargo"],
}
FACT_COLS = ["venta_id", "fecha_venta", "tienda_id", "producto_id", "cliente_id", "cantidad", "monto"]
BATCH_ORDER = ["b001", "b002", "b003", "b004", "b005", "b006"]
DIMS_VERSION = {"b001": "v1", "b002": "v1", "b003": "v1",
                "b004": "v2", "b005": "v2", "b006": "v2",
                "b006_malo": "v2", "b007_corrupto": "v2"}
UMBRAL_RECHAZO = 0.20
INVALID_TIENDA = 9999
INVALID_PRODUCTO = 9999
NEW_TIENDA = 13
NEW_PRODUCTO = 151


# ----------------------------------------------------------------------------- utilidades
def cents_to_str(c):
    sign = "-" if c < 0 else ""
    c = abs(c)
    return f"{sign}{c // 100}.{c % 100:02d}"


def str_to_cents(s):
    s = s.strip()
    neg = s.startswith("-")
    s = s.lstrip("-")
    if "." in s:
        a, b = s.split(".")
        b = (b + "00")[:2]
    else:
        a, b = s, "00"
    v = int(a) * 100 + int(b)
    return -v if neg else v


def write_csv(path, cols, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(cols)
        for r in rows:
            w.writerow([r.get(c, "") for c in cols])


def file_info(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        data = f.read()
    h.update(data)
    rows = data.decode("utf-8").count("\n") - 1
    return {"rows": rows, "bytes": len(data), "sha256": h.hexdigest()[:16]}


def clean_dim(name, rows):
    """Replica lo que hace Dataflow Gen2 (L1.2): trim, quitar duplicados por clave, nulos en ciudad."""
    key = DIM_KEYS[name]
    seen, out = set(), []
    for r in rows:
        r = {k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
        if r[key] in seen:
            continue
        seen.add(r[key])
        if name == "dim_tienda" and not r.get("ciudad"):
            r["ciudad"] = "Sin dato"
        out.append(r)
    return out


# ----------------------------------------------------------------------------- datos sinteticos
def synth_dims(rng):
    ciudades = ["San Salvador", "Santa Ana", "San Miguel", "Antiguo Cuscatlan", "Soyapango", "La Libertad"]
    formatos = ["Hiper", "Super", "Express"]
    tiendas = []
    for i in range(1, 13):
        tiendas.append({"tienda_id": str(i), "nombre": f"Aurora {ciudades[i % 6].split()[0]} {i}",
                        "ciudad": ciudades[i % 6], "formato": formatos[i % 3]})
    # suciedad v1 para el lab de Dataflow Gen2
    tiendas[2]["nombre"] = "  " + tiendas[2]["nombre"] + "  "
    tiendas[5]["ciudad"] = ""
    tiendas.append(dict(tiendas[0]))                    # duplicado exacto

    cats = ["Lacteos", "Panaderia", "Bebidas", "Limpieza", "Frutas", "Carnes", "Snacks", "Hogar"]
    productos = []
    for i in range(1, 151):
        costo = rng.randint(300, 9000)
        productos.append({"producto_id": str(i), "nombre": f"Producto {i:03d}", "categoria": cats[i % 8],
                          "costo_unitario": cents_to_str(costo),
                          "precio_lista": cents_to_str(int(costo * rng.uniform(1.35, 1.9)))})
    productos[10]["nombre"] = " Producto 011 "         # espacios
    segs = ["Premium", "Frecuente", "Ocasional", "Nuevo"]
    clientes = [{"cliente_id": str(i), "nombre": f"Cliente {i:04d}", "segmento": segs[i % 4]}
                for i in range(1, 2001)]
    emp = [{"empleado_id": str(i), "nombre": f"Empleado {i:03d}", "tienda_id": str(1 + i % 12),
            "cargo": ["Cajero", "Supervisor", "Gerente"][i % 3]} for i in range(1, 61)]
    return {"dim_tienda": tiendas, "dim_producto": productos, "dim_cliente": clientes, "dim_empleado": emp}


def make_dims_v2(dims_v1):
    v2 = {k: [dict(r) for r in v] for k, v in dims_v1.items()}
    t = dict(v2["dim_tienda"][0])
    t.update({"tienda_id": str(NEW_TIENDA), "nombre": "Aurora Norte 13", "ciudad": "Chalatenango"})
    v2["dim_tienda"].append(t)
    p0 = v2["dim_producto"]
    for r in p0[:5]:                                     # sube el costo 10 % en 5 productos
        r["costo_unitario"] = cents_to_str(int(str_to_cents(r["costo_unitario"]) * 1.10))
    nuevo = dict(p0[0])
    nuevo.update({"producto_id": str(NEW_PRODUCTO), "nombre": "Producto 151 (nuevo)",
                  "costo_unitario": "12.50", "precio_lista": "19.90"})
    p0.append(nuevo)
    return v2


def synth_facts(rng, n_rows, price_by_prod):
    start = dt.date(2024, 1, 1)
    end = dt.date(2025, 6, 30)
    days = (end - start).days + 1
    rows = []
    for i in range(n_rows):
        d = start + dt.timedelta(days=int(i * days / n_rows))     # ordenado por fecha
        pid = rng.randint(1, 150)
        qty = rng.randint(1, 6)
        unit = int(price_by_prod[str(pid)] * rng.uniform(0.9, 1.0))
        rows.append({"venta_id": f"V{i + 1:07d}", "fecha_venta": d.isoformat(),
                     "tienda_id": rng.randint(1, 12), "producto_id": pid,
                     "cliente_id": rng.randint(1, 2000), "cantidad": qty, "monto": unit * qty})
    return rows


# ----------------------------------------------------------------------------- datos reales (opcional)
def load_real(src, mapping_path):
    with open(mapping_path, encoding="utf-8") as f:
        mp = json.load(f)
    def read(entity):
        spec = mp[entity]
        path = os.path.join(src, spec["file"])
        with open(path, newline="", encoding=spec.get("encoding", "utf-8")) as f:
            rd = csv.DictReader(f)
            return [{canon: r[orig] for canon, orig in spec["columns"].items()} for r in rd]
    dims = {d: read(d) for d in DIMS}
    raw = read("fact_ventas")
    facts = []
    for r in raw:
        facts.append({"venta_id": r["venta_id"], "fecha_venta": r["fecha_venta"][:10],
                      "tienda_id": int(r["tienda_id"]), "producto_id": int(r["producto_id"]),
                      "cliente_id": int(r["cliente_id"]), "cantidad": int(float(r["cantidad"])),
                      "monto": str_to_cents(r["monto"])})
    facts.sort(key=lambda x: (x["fecha_venta"], x["venta_id"]))
    return dims, facts


# ----------------------------------------------------------------------------- construccion de lotes
def build_batches(rng, facts, n_hist_pct=0.85):
    n = len(facts)
    n_hist = int(n * n_hist_pct)
    hist = facts[:n_hist]
    rest = facts[n_hist:]
    k = len(rest) // 7
    slices = [rest[i * k:(i + 1) * k] for i in range(6)] + [rest[6 * k:]]

    def with_new_dims(sl):
        out = []
        for r in sl:
            r = dict(r)
            if rng.random() < 0.08:
                r["tienda_id"] = NEW_TIENDA
            if rng.random() < 0.03:
                r["producto_id"] = NEW_PRODUCTO
            out.append(r)
        return out

    canales = ["tienda", "online", "app"]
    def add_canal(rows):
        out = []
        for r in rows:
            r = dict(r)
            r["canal_venta"] = rng.choice(canales)
            out.append(r)
        return out

    b = {}
    b["b001"] = [dict(r) for r in hist]
    b["b002"] = [dict(r) for r in slices[0]]

    dup = rng.sample(b["b002"], max(5, int(len(b["b002"]) * 0.05)))
    b["b003"] = [dict(r) for r in slices[1]] + [dict(r) for r in dup]

    s3 = with_new_dims(slices[2])
    previos = b["b001"] + b["b002"] + b["b003"]
    corr_src = rng.sample(previos[:], 150)
    correcciones = []
    for j, r in enumerate(corr_src):
        r = dict(r)
        if j < 10:                                       # cambia de fecha (prueba fechas afectadas viejas+nuevas)
            d = dt.date.fromisoformat(r["fecha_venta"]) + dt.timedelta(days=1)
            r["fecha_venta"] = d.isoformat()
        elif j % 2 == 0:
            r["monto"] = int(r["monto"] * 1.10)
        else:
            unit = r["monto"] // r["cantidad"]
            r["cantidad"] += 1
            r["monto"] = unit * r["cantidad"]
        correcciones.append(r)
    b["b004"] = s3 + correcciones

    s4 = with_new_dims(slices[3])
    late = []
    for i in range(40):
        base = dict(rng.choice(b["b001"]))
        base["venta_id"] = f"L{i + 1:06d}"
        late.append(base)
    invalid = []
    def mk(i, tag):
        base = dict(rng.choice(s4))
        base["venta_id"] = f"X{tag}{i:03d}"
        return base
    for i in range(7):
        r = mk(i, "Q"); r["cantidad"] = rng.choice([0, -1, -2]); invalid.append(r)
    for i in range(7):
        r = mk(i, "T"); r["tienda_id"] = INVALID_TIENDA; invalid.append(r)
    for i in range(7):
        r = mk(i, "P"); r["producto_id"] = INVALID_PRODUCTO; invalid.append(r)
    for i in range(7):
        r = mk(i, "F"); r["fecha_venta"] = rng.choice(["N/A", "2025-02-30", "31/12/2024"]); invalid.append(r)
    for i in range(7):
        r = mk(i, "M"); r["monto"] = ""; invalid.append(r)
    for i in range(7):
        r = mk(i, "N"); r["venta_id"] = ""; invalid.append(r)
    infile_dups = []
    for r in rng.sample(s4, 8):                         # misma clave dos veces en el mismo archivo: gana la ultima
        r = dict(r)
        r["monto"] = int(r["monto"] * 1.05)
        infile_dups.append(r)
    b["b005"] = s4 + late + invalid + infile_dups

    b["b006"] = add_canal(with_new_dims(slices[4]))

    malo = add_canal([dict(r) for r in b["b006"]])
    for r in malo:
        r["canal_venta"] = r.get("canal_venta", "tienda")
    mal_idx = set(rng.sample(range(len(malo)), int(len(malo) * 0.30)))
    for i, r in enumerate(malo):
        if i in mal_idx:
            r["monto"] = ("COMMA", r["monto"])          # se formatea con coma al escribir
    b["b006_malo"] = malo

    corr = add_canal(with_new_dims(slices[5]))
    for r in rng.sample(corr, max(1, int(len(corr) * 0.03))):
        r["monto"] = r["monto"] * 100
    b["b007_corrupto"] = corr
    return b


def fmt_row(r, with_canal):
    out = {"venta_id": r["venta_id"], "fecha_venta": r["fecha_venta"], "tienda_id": r["tienda_id"],
           "producto_id": r["producto_id"], "cliente_id": r["cliente_id"], "cantidad": r["cantidad"]}
    m = r["monto"]
    if isinstance(m, tuple):
        out["monto"] = cents_to_str(m[1]).replace(".", ",")
    elif m == "":
        out["monto"] = ""
    else:
        out["monto"] = cents_to_str(m)
    if with_canal:
        out["canal_venta"] = r.get("canal_venta", "")
    return out


# ----------------------------------------------------------------------------- simulacion de referencia
def _int(s):
    try:
        return int(str(s).strip())
    except Exception:
        return None


def _date(s):
    s = str(s).strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return None
    try:
        return dt.date.fromisoformat(s).isoformat()
    except ValueError:
        return None


def _money(s):
    s = str(s).strip()
    if not re.fullmatch(r"-?\d+(\.\d{1,2})?", s):
        return None
    return str_to_cents(s)


def process_batch(rows_csv, dims_clean, silver, gold, bid, apply=True):
    """Replica nb_10_bronze + nb_20_silver + nb_30_gold sobre filas CSV (strings)."""
    tiendas = {r["tienda_id"] for r in dims_clean["dim_tienda"]}
    prod_cost = {r["producto_id"]: str_to_cents(r["costo_unitario"]) for r in dims_clean["dim_producto"]}
    n_in = len(rows_csv)
    typed = []
    for idx, r in enumerate(rows_csv):
        vid = (r.get("venta_id") or "").strip() or None
        key = vid if vid else f"__null__{idx}"
        typed.append({"key": key, "venta_id": vid, "fecha": _date(r["fecha_venta"]),
                      "tienda": _int(r["tienda_id"]), "producto": _int(r["producto_id"]),
                      "cliente": _int(r["cliente_id"]), "cantidad": _int(r["cantidad"]),
                      "monto": _money(r["monto"]), "idx": idx})
    last = {}
    for t in typed:                                       # gana la ultima aparicion
        last[t["key"]] = t
    dedup = sorted(last.values(), key=lambda t: t["idx"])
    ok, rej = [], Counter()
    n_rej = 0
    for t in dedup:
        motivos = []
        if t["venta_id"] is None:
            motivos.append("venta_id_nulo")
        if t["fecha"] is None:
            motivos.append("fecha_invalida")
        if t["cantidad"] is None or t["cantidad"] <= 0:
            motivos.append("cantidad_invalida")
        if t["monto"] is None:
            motivos.append("monto_invalido")
        if t["tienda"] is None or str(t["tienda"]) not in tiendas:
            motivos.append("tienda_inexistente")
        if t["producto"] is None or str(t["producto"]) not in prod_cost:
            motivos.append("producto_inexistente")
        if motivos:
            n_rej += 1
            rej[";".join(motivos)] += 1
        else:
            ok.append(t)
    res = {"bronze_rows": n_in, "silver_dedup_rows": len(dedup), "rejected": n_rej,
           "rejects_by_motivo": dict(sorted(rej.items()))}
    gate_fail = n_in > 0 and (n_rej / n_in) > UMBRAL_RECHAZO
    res["gate_abort"] = gate_fail
    if gate_fail:
        return res
    ins = upd = unch = 0
    fechas = set(t["fecha"] for t in ok)
    for t in ok:
        if t["venta_id"] in silver:
            fechas.add(silver[t["venta_id"]]["fecha"])
    new_silver = silver if apply else dict(silver)
    new_gold = gold if apply else dict(gold)
    touched = []
    for t in ok:
        h = (t["fecha"], t["tienda"], t["producto"], t["cliente"], t["cantidad"], t["monto"])
        cur = new_silver.get(t["venta_id"])
        if cur is None:
            ins += 1
        elif cur["h"] != h:
            upd += 1
        else:
            unch += 1
            continue
        new_silver[t["venta_id"]] = {"h": h, "fecha": t["fecha"], "tienda": t["tienda"],
                                     "producto": t["producto"], "monto": t["monto"], "cantidad": t["cantidad"]}
        touched.append(t["venta_id"])
    for vid in touched:                                   # gold: solo filas tocadas en este lote
        s = new_silver[vid]
        new_gold[vid] = {"fecha": s["fecha"], "tienda": s["tienda"], "monto": s["monto"],
                         "margen": s["monto"] - s["cantidad"] * prod_cost[str(s["producto"])]}
    res.update({"inserted": ins, "updated": upd, "unchanged": unch, "affected_dates": len(fechas),
                "silver_total": len(new_silver),
                "silver_sum_monto": cents_to_str(sum(v["monto"] for v in new_silver.values())),
                "gold_total": len(new_gold),
                "gold_sum_monto": cents_to_str(sum(v["monto"] for v in new_gold.values())),
                "gold_sum_margen": cents_to_str(sum(v["margen"] for v in new_gold.values())),
                "agg_dia_tienda_rows": len({(v["fecha"], v["tienda"]) for v in new_gold.values()})})
    return res


def read_rows(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ----------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=".", help="carpeta raiz del repo de datos")
    ap.add_argument("--rows", type=int, default=120000, help="filas de ventas (modo sintetico)")
    ap.add_argument("--seed", type=int, default=635)
    ap.add_argument("--mapping", help="mapping.json para datos reales")
    ap.add_argument("--src", default=".", help="carpeta con los CSV reales (con --mapping)")
    a = ap.parse_args()

    rng = random.Random(a.seed)
    if a.mapping:
        dims_v1, facts = load_real(a.src, a.mapping)
        mode = "real"
    else:
        dims_v1 = synth_dims(rng)
        prices = {r["producto_id"]: str_to_cents(r["precio_lista"]) for r in dims_v1["dim_producto"]}
        facts = synth_facts(rng, a.rows, prices)
        mode = "synthetic"
    dims_v2 = make_dims_v2(dims_v1)
    batches = build_batches(rng, facts)

    out = a.out
    for ver, dd in (("v1", dims_v1), ("v2", dims_v2)):
        for d in DIMS:
            write_csv(os.path.join(out, "dims", ver, f"{d}.csv"), DIM_COLS[d], dd[d])
    for bid, rows in batches.items():
        canal = bid in ("b006", "b006_malo", "b007_corrupto")
        cols = FACT_COLS + (["canal_venta"] if canal else [])
        write_csv(os.path.join(out, "batches", bid, "fact_ventas.csv"), cols, [fmt_row(r, canal) for r in rows])

    clean = {"v1": {d: clean_dim(d, dims_v1[d]) for d in DIMS},
             "v2": {d: clean_dim(d, dims_v2[d]) for d in DIMS}}
    expected = {"meta": {"mode": mode, "seed": a.seed, "rows_total": len(facts),
                         "umbral_rechazo": UMBRAL_RECHAZO,
                         "nota": "Conteos de referencia calculados por una simulacion en Python de nb_10/20/30."},
                "dims": {v: {d: {"raw_rows": len((dims_v1 if v == 'v1' else dims_v2)[d]),
                                 "clean_rows": len(clean[v][d])} for d in DIMS} for v in ("v1", "v2")},
                "files": {}, "batches": {}, "drills": {}}
    for ver in ("v1", "v2"):
        for d in DIMS:
            p = os.path.join(out, "dims", ver, f"{d}.csv")
            expected["files"][f"dims/{ver}/{d}.csv"] = file_info(p)
    silver, gold = {}, {}
    for bid in BATCH_ORDER:
        p = os.path.join(out, "batches", bid, "fact_ventas.csv")
        expected["files"][f"batches/{bid}/fact_ventas.csv"] = file_info(p)
        r = process_batch(read_rows(p), clean[DIMS_VERSION[bid]], silver, gold, bid, apply=True)
        r["dims_version"] = DIMS_VERSION[bid]
        expected["batches"][bid] = r
    for bid in ("b006_malo", "b007_corrupto"):
        p = os.path.join(out, "batches", bid, "fact_ventas.csv")
        expected["files"][f"batches/{bid}/fact_ventas.csv"] = file_info(p)
        r = process_batch(read_rows(p), clean[DIMS_VERSION[bid]], silver, gold, bid, apply=False)
        r["nota"] = ("Se simula sobre el estado posterior a b006 SIN aplicarlo. "
                     + ("Debe abortar en silver (quality gate)." if bid == "b006_malo"
                        else "Pasa todas las validaciones; el desvio solo se ve comparando totales."))
        expected["drills"][bid] = r
    with open(os.path.join(out, "expected.json"), "w", encoding="utf-8") as f:
        json.dump(expected, f, indent=2, ensure_ascii=False)
    print(f"OK ({mode}): {len(facts)} ventas -> {out}")
    for bid, r in expected["batches"].items():
        print(f"  {bid}: bronze={r['bronze_rows']} rej={r['rejected']} ins={r.get('inserted')} "
              f"upd={r.get('updated')} silver={r.get('silver_total')} gold_monto={r.get('gold_sum_monto')}")


if __name__ == "__main__":
    main()
