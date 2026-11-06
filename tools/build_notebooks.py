#!/usr/bin/env python3
"""
build_notebooks.py — Genera los .ipynb de soluciones y starters (con TODO) del Curso 5.

Cada notebook se define UNA sola vez. Los bloques marcados
    #@@TODO[descripcion]
    ...codigo de la solucion...
    #@@END
se dejan tal cual en  notebooks/solutions/  y se reemplazan por un TODO en  notebooks/starters/.

Uso:  python tools/build_notebooks.py          (escribe en ./notebooks)
"""
import json
import os
import re
import sys

NB_META = {
    "kernel_info": {"name": "synapse_pyspark"},
    "kernelspec": {"name": "synapse_pyspark", "language": "Python", "display_name": "Synapse PySpark"},
    "language_info": {"name": "python"},
    "microsoft": {"language": "python", "language_group": "synapse_pyspark"},
}


def md(text):
    return {"cell_type": "markdown", "metadata": {}, "source": text.strip("\n").splitlines(True)}


def code(text, params=False, name=None):
    meta = {}
    if params:
        meta["tags"] = ["parameters"]
    if name:
        meta["aurora_cell"] = name
    return {"cell_type": "code", "metadata": meta, "execution_count": None, "outputs": [],
            "source": text.strip("\n").splitlines(True)}


TODO_RE = re.compile(r"^([ \t]*)#@@TODO\[(.*?)\]\n(.*?)^[ \t]*#@@END[ \t]*\n?", re.S | re.M)


def to_solution(src):
    return TODO_RE.sub(lambda m: m.group(3), src)


def to_starter(src):
    def rep(m):
        ind = m.group(1)
        return (f"{ind}# TODO: {m.group(2)}\n{ind}# (completa aqui el codigo)\n{ind}pass  # borra este pass al completar\n")
    return TODO_RE.sub(rep, src)


def render(cells, mode):
    out = []
    for c in cells:
        c = json.loads(json.dumps(c))
        if c["cell_type"] == "code":
            s = "".join(c["source"]) + "\n"
            s = to_solution(s) if mode == "solutions" else to_starter(s)
            c["source"] = s.strip("\n").splitlines(True)
        out.append(c)
    return {"cells": out, "metadata": NB_META, "nbformat": 4, "nbformat_minor": 5}


# =====================================================================================
NOTEBOOKS = {}

# ------------------------------------------------------------------------- nb_utils
NOTEBOOKS["nb_utils"] = [
    md("""# nb_utils
Funciones compartidas. Se invoca desde los otros notebooks con `%run nb_utils`.
Requiere `lh_aurora` como Lakehouse predeterminado. **Sin TODO: se entrega igual en starters y soluciones.**"""),
    code(r'''
from pyspark.sql import functions as F

FMT = "delta"          # formato de las tablas

def ensure_control_tables():
    """Crea las tablas de control si no existen (evita 'Table not found' en la primera corrida)."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS ctl_batch_plan
                  (batch_id STRING, orden INT, dims_version STRING, descripcion STRING) USING {FMT}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS ctl_batch_log
                  (batch_id STRING, status STRING, rows_in BIGINT, rows_ok BIGINT, rows_rejected BIGINT,
                   run_id STRING, msg STRING, updated_at TIMESTAMP) USING {FMT}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS ctl_batch_dates
                  (batch_id STRING, fecha DATE) USING {FMT}""")

def log_estado(batch_id, status, rows_in=None, rows_ok=None, rows_rej=None, run_id=None, msg=None):
    """Log append-only: el estado vigente de un lote es su ultimo evento."""
    ensure_control_tables()
    (spark.createDataFrame(
        [(batch_id, status, rows_in, rows_ok, rows_rej, run_id, msg)],
        "batch_id string, status string, rows_in long, rows_ok long, rows_rejected long, run_id string, msg string")
     .withColumn("updated_at", F.current_timestamp())
     .write.format(FMT).mode("append").saveAsTable("ctl_batch_log"))

def write_batch(df, table, batch_id, col="_batch_id"):
    """Escritura idempotente por lote: reemplaza SOLO las filas de ese lote (replaceWhere)."""
    w = df.write.format(FMT).option("mergeSchema", "true")
    if spark.catalog.tableExists(table):
        w.mode("overwrite").option("replaceWhere", f"{col} = '{batch_id}'").saveAsTable(table)
    else:
        w.mode("overwrite").saveAsTable(table)
'''),
]

# ------------------------------------------------------------------------- nb_00_control
NOTEBOOKS["nb_00_control"] = [
    md("""# nb_00_control — Lab L2.1
Decide **que lote toca**. Regla: un lote esta terminado solo si existe `gold_ok` en `ctl_batch_log`.
Devuelve por `notebookutils.notebook.exit` un JSON `{hay_datos, batch_id, dims_version}`."""),
    code('''
modo = "next"            # next | override
batch_override = ""      # solo se usa con modo = "override"
''', params=True),
    code("%run nb_utils"),
    code(r'''
import json
ensure_control_tables()

if spark.table("ctl_batch_plan").count() == 0:
    #@@TODO[Sembrar ctl_batch_plan: b001..b006 con orden, dims_version (v1 hasta b003, v2 desde b004) y descripcion]
    spark.sql("""INSERT INTO ctl_batch_plan VALUES
      ('b001',1,'v1','Carga historica'),
      ('b002',2,'v1','Incremento limpio'),
      ('b003',3,'v1','Duplicados entre lotes'),
      ('b004',4,'v2','Correcciones + dims v2'),
      ('b005',5,'v2','Tardias + invalidas'),
      ('b006',6,'v2','Schema drift')""")
    #@@END

#@@TODO[Elegir el lote. override: el pedido (si existe en el plan). next: el primero por 'orden' que NO tenga status 'gold_ok' en ctl_batch_log]
if modo == "override" and batch_override:
    fila = spark.sql(f"SELECT batch_id, dims_version FROM ctl_batch_plan WHERE batch_id = '{batch_override}'").collect()
else:
    fila = spark.sql("""
        SELECT p.batch_id, p.dims_version
        FROM ctl_batch_plan p
        WHERE NOT EXISTS (SELECT 1 FROM ctl_batch_log l
                          WHERE l.batch_id = p.batch_id AND l.status = 'gold_ok')
        ORDER BY p.orden LIMIT 1""").collect()
#@@END

if fila:
    out = {"hay_datos": "true", "batch_id": fila[0]["batch_id"], "dims_version": fila[0]["dims_version"]}
else:
    out = {"hay_datos": "false", "batch_id": "", "dims_version": ""}

print(out)
notebookutils.notebook.exit(json.dumps(out))
'''),
]

# ------------------------------------------------------------------------- nb_10_bronze
NOTEBOOKS["nb_10_bronze"] = [
    md("""# nb_10_bronze — Lab L1.3 / L2.2
Landing (CSV) -> `brz_fact_ventas`. **Regla de Bronze: todo como string, nada se interpreta.**
Idempotente: re-ejecutar el mismo `batch_id` reemplaza solo ese lote."""),
    code('''
batch_id = "b001"
''', params=True),
    code("%run nb_utils"),
    code(r'''
from pyspark.sql import functions as F

ENTIDAD = "fact_ventas"
ruta = f"Files/raw_data/landing/{batch_id}/{ENTIDAD}.csv"

try:
    #@@TODO[Leer el CSV con header, SIN inferir esquema (todo string)]
    df = (spark.read.option("header", True).option("inferSchema", False).csv(ruta))
    #@@END

    #@@TODO[Agregar columnas de trazabilidad: _batch_id, _ingest_ts, _row_id (monotonically_increasing_id) y _source_file (_metadata.file_path)]
    df = (df.withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_row_id", F.monotonically_increasing_id())
            .withColumn("_source_file", F.col("_metadata.file_path")))
    #@@END
    n = df.count()

    #@@TODO[Escribir de forma IDEMPOTENTE con write_batch (replaceWhere por lote). NO usar mode("append")]
    write_batch(df, "brz_fact_ventas", batch_id)
    #@@END

    log_estado(batch_id, "bronze_ok", rows_in=n)
    print(f"bronze_ok {batch_id}: {n} filas")
except Exception as e:
    log_estado(batch_id, "failed_bronze", msg=str(e)[:500])
    raise
'''),
]

# ------------------------------------------------------------------------- nb_20_silver
NOTEBOOKS["nb_20_silver"] = [
    md("""# nb_20_silver — Lab L1.3 / L2.3 / L2.5
`brz_fact_ventas` -> `slv_fact_ventas` (+ `slv_rejects_fact_ventas`).
Pasos: tipar -> dedupe dentro del lote -> validar (incl. FK a dims) -> rechazos -> **quality gate** -> fechas afectadas -> MERGE.
Contrato explicito: columnas nuevas en Bronze (schema drift) **no** pasan a Silver hasta que se decida."""),
    code('''
batch_id = "b001"
''', params=True),
    code("%run nb_utils"),
    code(r'''
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

UMBRAL_RECHAZO = 0.20
COLS_HASH = ["fecha_venta", "tienda_id", "producto_id", "cliente_id", "cantidad", "monto"]

def transformar_silver(b, dim_t, dim_p):
    """Bronze (strings) -> (ok, rechazos, n_tras_dedupe). Funcion pura: no escribe nada."""
    cols_neg = [c for c in b.columns if not c.startswith("_")]
    b = b.withColumn("_raw", F.to_json(F.struct(*[F.col(c) for c in cols_neg])))

    #@@TODO[Tipar con try_cast (nunca cast directo): venta_id string, fecha_venta date, tienda_id/producto_id/cliente_id/cantidad int, monto decimal(18,2). Conservar _batch_id, _ingest_ts, _row_id, _raw]
    t = b.select(
        F.col("venta_id").cast("string").alias("venta_id"),
        F.expr("try_cast(fecha_venta as date)").alias("fecha_venta"),
        F.expr("try_cast(tienda_id as int)").alias("tienda_id"),
        F.expr("try_cast(producto_id as int)").alias("producto_id"),
        F.expr("try_cast(cliente_id as int)").alias("cliente_id"),
        F.expr("try_cast(cantidad as int)").alias("cantidad"),
        F.expr("try_cast(monto as decimal(18,2))").alias("monto"),
        "_batch_id", "_ingest_ts", "_row_id", "_raw")
    #@@END

    #@@TODO[Dedupe dentro del lote por venta_id: gana la ULTIMA aparicion (_row_id desc). Ojo: las filas con venta_id nulo NO deben colapsar entre si]
    clave = F.coalesce(F.col("venta_id"), F.concat(F.lit("__null__"), F.col("_row_id").cast("string")))
    w = Window.partitionBy(clave).orderBy(F.col("_row_id").desc())
    t = t.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    #@@END
    n_dedup = t.count()

    #@@TODO[Validar FK contra las dims y armar _motivo con concat_ws: venta_id_nulo, fecha_invalida, cantidad_invalida (nula o <=0), monto_invalido, tienda_inexistente, producto_inexistente]
    t = t.join(dim_t, "tienda_id", "left").join(dim_p, "producto_id", "left")
    motivo = F.concat_ws(";",
        F.when(F.col("venta_id").isNull() | (F.col("venta_id") == ""), F.lit("venta_id_nulo")),
        F.when(F.col("fecha_venta").isNull(), F.lit("fecha_invalida")),
        F.when(F.col("cantidad").isNull() | (F.col("cantidad") <= 0), F.lit("cantidad_invalida")),
        F.when(F.col("monto").isNull(), F.lit("monto_invalido")),
        F.when(F.col("_ok_t").isNull(), F.lit("tienda_inexistente")),
        F.when(F.col("_ok_p").isNull(), F.lit("producto_inexistente")))
    t = t.withColumn("_motivo", motivo)
    #@@END

    rech = t.where(F.col("_motivo") != "").select("venta_id", "_motivo", "_raw", "_batch_id", "_ingest_ts")

    #@@TODO[Filas validas: row_hash = sha2 de las COLS_HASH casteadas a string unidas con '|', y updated_at = current_timestamp()]
    ok = (t.where(F.col("_motivo") == "")
           .withColumn("row_hash", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in COLS_HASH]), 256))
           .withColumn("updated_at", F.current_timestamp())
           .select("venta_id", "fecha_venta", "tienda_id", "producto_id", "cliente_id",
                   "cantidad", "monto", "row_hash", "_batch_id", "_ingest_ts", "updated_at"))
    #@@END
    return ok, rech, n_dedup
'''),
    code(r'''
ensure_control_tables()
try:
    b = spark.table("brz_fact_ventas").where(F.col("_batch_id") == batch_id)
    n_in = b.count()
    # las dims las escribe Dataflow Gen2; se castea la clave por si el dataflow la dejo como texto
    dim_t = (spark.table("slv_dim_tienda").select(F.col("tienda_id").cast("int").alias("tienda_id"))
             .distinct().withColumn("_ok_t", F.lit(1)))
    dim_p = (spark.table("slv_dim_producto").select(F.col("producto_id").cast("int").alias("producto_id"))
             .distinct().withColumn("_ok_p", F.lit(1)))

    ok, rech, n_dedup = transformar_silver(b, dim_t, dim_p)
    n_rej = rech.count()
    write_batch(rech, "slv_rejects_fact_ventas", batch_id)      # primero la evidencia

    #@@TODO[Quality gate: si rechazadas/n_in supera UMBRAL_RECHAZO, lanzar Exception ANTES de tocar slv_fact_ventas]
    if n_in > 0 and (n_rej / n_in) > UMBRAL_RECHAZO:
        raise Exception(f"Quality gate: {n_rej}/{n_in} rechazadas (> {UMBRAL_RECHAZO:.0%})")
    #@@END

    #@@TODO[Fechas afectadas = fechas de las filas nuevas + fechas que ya tenian en silver esas mismas claves (por si cambian de fecha). Guardarlas en ctl_batch_dates con write_batch(col="batch_id")]
    nuevas = ok.select(F.col("fecha_venta").alias("fecha"))
    if spark.catalog.tableExists("slv_fact_ventas"):
        previas = (ok.select("venta_id").join(spark.table("slv_fact_ventas"), "venta_id")
                     .select(F.col("fecha_venta").alias("fecha")))
        fechas = nuevas.union(previas).distinct()
    else:
        fechas = nuevas.distinct()
    write_batch(fechas.withColumn("batch_id", F.lit(batch_id)), "ctl_batch_dates", batch_id, col="batch_id")
    #@@END

    #@@TODO[MERGE a slv_fact_ventas por venta_id. UPDATE solo si el row_hash cambio (y el lote es igual o mas nuevo). INSERT si no existe]
    if not spark.catalog.tableExists("slv_fact_ventas"):
        ok.limit(0).write.format(FMT).saveAsTable("slv_fact_ventas")
    (DeltaTable.forName(spark, "slv_fact_ventas").alias("t")
        .merge(ok.alias("s"), "t.venta_id = s.venta_id")
        .whenMatchedUpdateAll(condition="t.row_hash <> s.row_hash AND s._batch_id >= t._batch_id")
        .whenNotMatchedInsertAll()
        .execute())
    #@@END

    n_ok = ok.count()
    log_estado(batch_id, "silver_ok", rows_in=n_in, rows_ok=n_ok, rows_rej=n_rej)
    print(f"silver_ok {batch_id}: in={n_in} dedupe={n_dedup} ok={n_ok} rechazadas={n_rej}")
except Exception as e:
    log_estado(batch_id, "failed_silver", msg=str(e)[:500])
    raise
'''),
]

# ------------------------------------------------------------------------- nb_30_gold
NOTEBOOKS["nb_30_gold"] = [
    md("""# nb_30_gold — Lab L1.4 / L2.4  (Spark SQL)
Silver -> Gold. Dims: recompute completo (son chicas). Hecho: `MERGE` solo con filas del lote.
Agregado dia x tienda: `DELETE + INSERT` por **fechas afectadas** (idempotente). Agregado mes x categoria: recompute completo (contraste didactico).
Las columnas son **placeholders**: ajustalas al schema real de Aurora."""),
    code('''
batch_id = "b001"
''', params=True),
    code("%run nb_utils"),
    code(r'''
def sql_fact_select(b):
    """Filas de silver tocadas por el lote b, con margen = monto - cantidad * costo_unitario."""
    return f"""
      SELECT v.venta_id, v.fecha_venta, v.tienda_id, v.producto_id, v.cliente_id, v.cantidad, v.monto,
             CAST(v.monto - v.cantidad * CAST(p.costo_unitario AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS margen,
             v.updated_at
      FROM slv_fact_ventas v
      JOIN slv_dim_producto p ON CAST(p.producto_id AS INT) = v.producto_id
      WHERE v._batch_id = '{b}'"""

def sql_agg_dia_tienda(b):
    """Recalculo del agregado SOLO para las fechas afectadas por el lote b."""
    return f"""
      SELECT fecha_venta, tienda_id,
             CAST(COUNT(*) AS BIGINT) AS n_ventas, CAST(SUM(cantidad) AS BIGINT) AS unidades,
             CAST(SUM(monto) AS DECIMAL(18,2)) AS monto, CAST(SUM(margen) AS DECIMAL(18,2)) AS margen,
             current_timestamp() AS updated_at
      FROM gld_fact_ventas
      WHERE fecha_venta IN (SELECT fecha FROM ctl_batch_dates WHERE batch_id = '{b}')
      GROUP BY fecha_venta, tienda_id"""
'''),
    code(r'''
ensure_control_tables()
b = batch_id
try:
    spark.sql("""CREATE TABLE IF NOT EXISTS gld_fact_ventas (
        venta_id STRING, fecha_venta DATE, tienda_id INT, producto_id INT, cliente_id INT,
        cantidad INT, monto DECIMAL(18,2), margen DECIMAL(18,2), updated_at TIMESTAMP) USING DELTA""")
    spark.sql("""CREATE TABLE IF NOT EXISTS gld_agg_ventas_dia_tienda (
        fecha_venta DATE, tienda_id INT, n_ventas BIGINT, unidades BIGINT,
        monto DECIMAL(18,2), margen DECIMAL(18,2), updated_at TIMESTAMP) USING DELTA""")

    #@@TODO[Calendario con sequence() entre 2022-01-01 y 2026-12-31: fecha, anio, trimestre, mes_num, mes, anio_mes, dia_semana]
    spark.sql("""CREATE OR REPLACE TABLE gld_dim_calendario AS
        SELECT d AS fecha, year(d) AS anio, quarter(d) AS trimestre, month(d) AS mes_num,
               date_format(d,'MMMM') AS mes, date_format(d,'yyyy-MM') AS anio_mes, dayofweek(d) AS dia_semana
        FROM (SELECT explode(sequence(to_date('2022-01-01'), to_date('2026-12-31'), interval 1 day)) AS d)""")
    #@@END

    #@@TODO[Dims gold con CREATE OR REPLACE TABLE ... AS SELECT, casteando la clave a INT]
    spark.sql("""CREATE OR REPLACE TABLE gld_dim_tienda AS
        SELECT CAST(tienda_id AS INT) AS tienda_id, nombre, ciudad, formato FROM slv_dim_tienda""")
    spark.sql("""CREATE OR REPLACE TABLE gld_dim_producto AS
        SELECT CAST(producto_id AS INT) AS producto_id, nombre, categoria,
               CAST(costo_unitario AS DECIMAL(18,2)) AS costo_unitario,
               CAST(precio_lista AS DECIMAL(18,2)) AS precio_lista FROM slv_dim_producto""")
    spark.sql("""CREATE OR REPLACE TABLE gld_dim_cliente AS
        SELECT CAST(cliente_id AS INT) AS cliente_id, nombre, segmento FROM slv_dim_cliente""")
    #@@END

    #@@TODO[MERGE INTO gld_fact_ventas usando sql_fact_select(b): UPDATE SET * / INSERT *]
    spark.sql(f"""
    MERGE INTO gld_fact_ventas t
    USING ({sql_fact_select(b)}) s
    ON t.venta_id = s.venta_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
    #@@END

    #@@TODO[Agregado dia x tienda: DELETE de las fechas afectadas (ctl_batch_dates) + INSERT con sql_agg_dia_tienda(b)]
    spark.sql(f"""DELETE FROM gld_agg_ventas_dia_tienda
                  WHERE fecha_venta IN (SELECT fecha FROM ctl_batch_dates WHERE batch_id = '{b}')""")
    spark.sql(f"INSERT INTO gld_agg_ventas_dia_tienda {sql_agg_dia_tienda(b)}")
    #@@END

    #@@TODO[Agregado mes x categoria: CREATE OR REPLACE TABLE (recompute completo) uniendo con gld_dim_producto]
    spark.sql("""CREATE OR REPLACE TABLE gld_agg_ventas_mes_categoria AS
        SELECT date_format(f.fecha_venta,'yyyy-MM') AS anio_mes, p.categoria,
               COUNT(*) AS n_ventas, SUM(f.cantidad) AS unidades, SUM(f.monto) AS monto, SUM(f.margen) AS margen
        FROM gld_fact_ventas f JOIN gld_dim_producto p ON p.producto_id = f.producto_id
        GROUP BY 1, 2""")
    #@@END

    a = spark.sql("SELECT COUNT(*) c, SUM(monto) m FROM gld_fact_ventas").first()
    s = spark.sql("SELECT COUNT(*) c, SUM(monto) m FROM slv_fact_ventas").first()
    if a["c"] != s["c"] or a["m"] != s["m"]:
        raise Exception(f"Reconciliacion gold vs silver: {a['c']}/{a['m']} vs {s['c']}/{s['m']}")

    log_estado(batch_id, "gold_ok", rows_ok=a["c"])
    print(f"gold_ok {batch_id}: filas={a['c']} monto={a['m']}")
except Exception as e:
    log_estado(batch_id, "failed_gold", msg=str(e)[:500])
    raise
'''),
]

# ------------------------------------------------------------------------- nb_95_mantenimiento
NOTEBOOKS["nb_95_mantenimiento"] = [
    md("""# nb_95_mantenimiento — Lab L4.5
`OPTIMIZE` + `VACUUM` y recuperacion por time travel. Programalo aparte (semanal), no dentro del pipeline diario."""),
    code(r'''
tablas = ["gld_fact_ventas", "gld_agg_ventas_dia_tienda", "gld_agg_ventas_mes_categoria",
          "slv_fact_ventas", "brz_fact_ventas"]
for t in tablas:
    antes = spark.sql(f"DESCRIBE DETAIL {t}").select("numFiles", "sizeInBytes").first()
    spark.sql(f"OPTIMIZE {t}")
    spark.sql(f"VACUUM {t} RETAIN 168 HOURS")      # 7 dias: NO bajar de aca si queres conservar time travel
    despues = spark.sql(f"DESCRIBE DETAIL {t}").select("numFiles", "sizeInBytes").first()
    print(f"{t}: archivos {antes['numFiles']} -> {despues['numFiles']}, bytes {antes['sizeInBytes']} -> {despues['sizeInBytes']}")
'''),
    md("""## Recuperacion (drill D4) — ejecutar celda por celda, A MANO
1. Ver historial y elegir la version ANTERIOR al lote corrupto en **ambas** tablas.
2. `RESTORE` en silver y gold al mismo punto en el tiempo.
3. Registrar el lote como fallido en el log para que el control lo vuelva a procesar."""),
    code('''
display(spark.sql("DESCRIBE HISTORY slv_fact_ventas"))
display(spark.sql("DESCRIBE HISTORY gld_fact_ventas"))
'''),
    code('''
# Completa con las versiones que elegiste mirando el historial:
V_SILVER = None
V_GOLD = None
BATCH_MALO = "b007_corrupto"
assert V_SILVER is not None and V_GOLD is not None, "Define las versiones antes de restaurar"
spark.sql(f"RESTORE TABLE slv_fact_ventas TO VERSION AS OF {V_SILVER}")
spark.sql(f"RESTORE TABLE gld_fact_ventas TO VERSION AS OF {V_GOLD}")
'''),
    code("%run nb_utils"),
    code('''
log_estado(BATCH_MALO, "failed_manual", msg="Restaurado por reconciliacion")
'''),
]

# ------------------------------------------------------------------------- nb_reset_demo
NOTEBOOKS["nb_reset_demo"] = [
    md("""# nb_reset_demo
Deja el proyecto listo para empezar de cero: borra Bronze/Silver de hechos, Gold y control.
**No** borra `slv_dim_*` (las genera el Dataflow) ni los archivos de `Files`. Es destructivo: confirmalo con `CONFIRMAR = True`."""),
    code('''
CONFIRMAR = False
tablas = ["brz_fact_ventas", "slv_fact_ventas", "slv_rejects_fact_ventas",
          "gld_fact_ventas", "gld_agg_ventas_dia_tienda", "gld_agg_ventas_mes_categoria",
          "gld_dim_calendario", "gld_dim_tienda", "gld_dim_producto", "gld_dim_cliente",
          "ctl_batch_plan", "ctl_batch_log", "ctl_batch_dates"]
assert CONFIRMAR, "Poner CONFIRMAR = True para ejecutar"
for t in tablas:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    print("borrada", t)
'''),
]

# ------------------------------------------------------------------------- nb_volumizar
NOTEBOOKS["nb_volumizar"] = [
    md("""# nb_volumizar — Lab L1.5 (preparacion del instructor)
Crea `gld_fact_ventas_xl`: el hecho multiplicado `K` veces y con **defectos de diseno a proposito**
(`venta_id_texto` de cardinalidad altisima, `fecha_hora_venta` con segundos, columna `comentarios` sin uso).
**No forma parte del pipeline.** Ajusta `K` segun la capacidad: 120 mil filas x 200 = 24 M."""),
    code('''
K = 200
''', params=True),
    code(r'''
from pyspark.sql import functions as F

base = spark.table("gld_fact_ventas")
xl = (base.crossJoin(spark.range(K).withColumnRenamed("id", "k"))
      .withColumn("venta_id_texto", F.concat(F.col("venta_id"), F.lit("-"), F.col("k").cast("string")))
      .withColumn("fecha_hora_venta",
                  (F.unix_timestamp(F.col("fecha_venta").cast("timestamp")) + (F.rand(7) * 86400).cast("long")).cast("timestamp"))
      .withColumn("comentarios", F.sha2(F.col("venta_id_texto"), 256))
      .drop("venta_id", "k"))
(xl.write.format("delta").mode("overwrite").saveAsTable("gld_fact_ventas_xl"))
print(spark.table("gld_fact_ventas_xl").count(), "filas")
'''),
]

# ------------------------------------------------------------------------- nb_restore
NOTEBOOKS["nb_restore"] = [
    md("""# nb_restore — recuperar el ambiente hasta un lote
Para alumnos que se atrasaron. Descarga de GitHub (si falta), reemplaza al Dataflow para las dims y corre bronze/silver/gold lote por lote.
- `hasta = "b001"` -> estado al terminar S1.  `hasta = "b006"` -> estado al terminar S2.
- Ajusta `github_base` a tu repo. Requiere `lh_aurora` como Lakehouse predeterminado.
- **Sin ejecutar todavia:** valida `requests` desde Spark en tu tenant (si la red lo bloquea, usa el Plan B de subir los archivos a mano)."""),
    code('''
hasta = "b001"
github_base = "https://raw.githubusercontent.com/<usuario-u-org>/aurora-curso5/main/"
''', params=True),
    code("%run nb_utils"),
    code(r'''
import os, requests
from pyspark.sql import functions as F

PLAN = [("b001", "v1"), ("b002", "v1"), ("b003", "v1"), ("b004", "v2"), ("b005", "v2"), ("b006", "v2")]
DIMS = {"dim_tienda": "tienda_id", "dim_producto": "producto_id", "dim_cliente": "cliente_id", "dim_empleado": "empleado_id"}
ROOT = "/lakehouse/default/Files/"

def descargar(rel_url, rel_dest):
    dest = ROOT + rel_dest
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    r = requests.get(github_base + rel_url, timeout=60)
    r.raise_for_status()
    with open(dest, "wb") as f:
        f.write(r.content)

def cargar_dims():
    """Hace lo mismo que el Dataflow Gen2: trim, quitar duplicados por clave, ciudad nula -> 'Sin dato'."""
    for d, k in DIMS.items():
        df = spark.read.option("header", True).csv(f"Files/raw_data/dims/{d}.csv")
        for c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
        df = df.dropDuplicates([k])
        if d == "dim_tienda":
            df = df.withColumn("ciudad", F.when(F.col("ciudad").isNull() | (F.col("ciudad") == ""), F.lit("Sin dato")).otherwise(F.col("ciudad")))
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"slv_dim_{d.replace('dim_', '')}")
'''),
    code(r'''
ensure_control_tables()
for bid, ver in PLAN:
    if bid > hasta:
        break
    descargar(f"batches/{bid}/fact_ventas.csv", f"raw_data/landing/{bid}/fact_ventas.csv")
    for d in DIMS:
        descargar(f"dims/{ver}/{d}.csv", f"raw_data/dims/{d}.csv")
    cargar_dims()
    for nb in ("nb_10_bronze", "nb_20_silver", "nb_30_gold"):
        notebookutils.notebook.run(nb, 1800, {"batch_id": bid})
    print("listo", bid)
'''),
]


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    n = 0
    for mode in ("solutions", "starters"):
        d = os.path.join(root, "notebooks", mode)
        os.makedirs(d, exist_ok=True)
        for name, cells in NOTEBOOKS.items():
            nb = render(cells, mode)
            with open(os.path.join(d, f"{name}.ipynb"), "w", encoding="utf-8") as f:
                json.dump(nb, f, indent=1, ensure_ascii=False)
            n += 1
    print(f"{n} notebooks escritos en {os.path.join(root, 'notebooks')}")


if __name__ == "__main__":
    main()
