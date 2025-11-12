from math import pi
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, to_timestamp, lag, when, lit, countDistinct, avg, count,
    round as sround, sin, cos, sqrt, asin, radians, explode
)
from s3_utils import get_spark_session  


SILVER_POSICAO = "s3a://silver/posicao"
GOLD_BASE      = "s3a://gold/sptrans/"

ITAQUERA_LAT_MIN, ITAQUERA_LAT_MAX = -23.60, -23.50
ITAQUERA_LON_MIN, ITAQUERA_LON_MAX = -46.50, -46.40

VEL_WINDOW_MIN = 5  # minutos
EARTH_R = 6371000.0  # metros

spark = get_spark_session()

def haversine_expr(lat1, lon1, lat2, lon2):
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * lit(EARTH_R) * asin(sqrt(a))


print("Lendo dados da camada Silver...")

from pyspark.sql.functions import explode

df_pos_base = spark.read.format("parquet").load(SILVER_POSICAO)

df_pos = (
    df_pos_base
    .withColumn("v", explode(col("veiculos")))
    .select(
        col("cl"),
        col("codigo_linha"),
        col("sentido_linha").alias("sl"),
        col("terminal_origem").alias("lt0"),
        col("terminal_destino").alias("lt1"),
        col("v.p").alias("veiculo_id"),
        col("v.py").alias("lat"),
        col("v.px").alias("lon"),
        col("v.a").alias("acessivel"),
        to_timestamp(col("v.ta")).alias("ts")
    )
    .where(col("ts").isNotNull())
)

print(f"Registros lidos da Silver (posicao): {df_pos.count()}")


df_itaquera = df_pos.where(
    (col("lat").between(ITAQUERA_LAT_MIN, ITAQUERA_LAT_MAX)) &
    (col("lon").between(ITAQUERA_LON_MIN, ITAQUERA_LON_MAX))
).cache()

print(f"Registros no recorte de Itaquera: {df_itaquera.count()}")


gold_positions = df_itaquera.select(
    "ts", "veiculo_id", "lat", "lon", "acessivel",
    "cl", "codigo_linha", "sl", "lt0", "lt1"
)
gold_positions.write.mode("overwrite").format("parquet").save(GOLD_BASE + "gold_frota_itaquera_positions")
print("Gold 1 - Frota dinâmica salva")


w = Window.partitionBy("veiculo_id").orderBy("ts")

df_speed = (
    df_itaquera
    .select("ts", "veiculo_id", "lat", "lon", "cl", "codigo_linha", "sl")
    .withColumn("lat_prev", lag("lat").over(w))
    .withColumn("lon_prev", lag("lon").over(w))
    .withColumn("ts_prev",  lag("ts").over(w))
    .where(col("lat_prev").isNotNull() & col("lon_prev").isNotNull() & col("ts_prev").isNotNull())
    .withColumn("dist_m", haversine_expr(col("lat_prev"), col("lon_prev"), col("lat"), col("lon")))
    .withColumn("dt_s", (col("ts").cast("long") - col("ts_prev").cast("long")).cast("double"))
    .withColumn("vel_ms", when(col("dt_s") > 0, col("dist_m")/col("dt_s")))
    .withColumn("vel_kmh", col("vel_ms") * lit(3.6))
    .withColumn("ts_s", col("ts").cast("long"))
)

w5 = (
    Window
    .partitionBy("veiculo_id", "cl", "codigo_linha", "sl")
    .orderBy("ts_s")
    .rangeBetween(-VEL_WINDOW_MIN * 60, 0)
)

vel_5min = (
    df_speed
    .withColumn("vel_kmh_5min", avg("vel_kmh").over(w5))
    .withColumn("velocidade_media", sround(col("vel_kmh_5min"), 2))
    .select(
        "ts",
        "veiculo_id",
        "cl",
        "codigo_linha",
        "sl",
        "vel_kmh",
        "vel_kmh_5min",
        "velocidade_media"
    )
)
vel_5min.write.mode("overwrite").format("parquet").save(GOLD_BASE + "gold_vel_media_5min_por_veiculo")
print("Gold 2a - Velocidade média por veículo salva")

w5_line = (
    Window
    .partitionBy("cl", "codigo_linha", "sl")
    .orderBy("ts_s")
    .rangeBetween(-VEL_WINDOW_MIN * 60, 0)
)

vel_5min_line = (
    df_speed
    .withColumn("vel_kmh_5min_linha", avg("vel_kmh").over(w5_line))
    .groupBy("ts", "cl", "codigo_linha", "sl")
    .agg(
        avg("vel_kmh").alias("vel_kmh_media_inst"),
        avg("vel_kmh_5min_linha").alias("vel_kmh_media_5min")
    )
    .withColumn("velocidade_media", sround(col("vel_kmh_media_5min"), 2))
)
vel_5min_line.write.mode("overwrite").format("parquet").save(GOLD_BASE + "gold_vel_media_5min_por_linha")
print("Gold 2b - Velocidade média por linha salva")

w_line = Window.partitionBy("cl", "codigo_linha", "sl").orderBy("ts")

df_headway = (
    df_itaquera
    .select("ts", "veiculo_id", "cl", "codigo_linha", "sl")
    .withColumn("veic_prev", lag("veiculo_id").over(w_line))
    .withColumn("ts_prev", lag("ts").over(w_line))
    .where(col("veic_prev").isNotNull() & (col("veic_prev") != col("veiculo_id")))
    .withColumn("headway_s_aprox", (col("ts").cast("long") - col("ts_prev").cast("long")).cast("double"))
)
df_headway.write.mode("overwrite").format("parquet").save(GOLD_BASE + "gold_headway_aproximado")
print("Gold 3 - Headway aproximado salvo")


GRID_PREC = 3  
df_grid = (
    df_itaquera
    .withColumn("lat_g", sround(col("lat"), GRID_PREC))
    .withColumn("lon_g", sround(col("lon"), GRID_PREC))
    .groupBy("ts", "lat_g", "lon_g")
    .agg(count("*").alias("qtd_veiculos"))
)
df_grid.write.mode("overwrite").format("parquet").save(GOLD_BASE + "gold_densidade_grid")
print("Gold 4 - Densidade de veículos salva")


df_access = (
    df_itaquera
    .groupBy("ts")
    .agg(
        (avg(when(col("acessivel") == True, lit(1)).otherwise(lit(0))) * 100.0).alias("pct_acessivel"),
        countDistinct("veiculo_id").alias("frota_ativa")
    )
)
df_access.write.mode("overwrite").format("parquet").save(GOLD_BASE + "gold_acessibilidade_share")
print("Gold 5 - Acessibilidade salva")

spark.stop()
print("Pipeline GOLD finalizado com sucesso.")
