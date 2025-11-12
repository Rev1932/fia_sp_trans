import os
from pathlib import Path
import duckdb
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()
MINIO_ENDPOINT_URL = os.environ.get('MINIO_ENDPOINT_URL')
MINIO_USER = os.environ.get('MINIO_USER')
MINIO_PASSWORD = os.environ.get('MINIO_PASSWORD')


def get_duckdb_connection(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Cria conexão DuckDB configurada para acessar o MinIO via S3A"""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    endpoint_without_protocol = MINIO_ENDPOINT_URL.split("//", 1)[-1]
    use_ssl = 'true' if MINIO_ENDPOINT_URL.lower().startswith('https://') else 'false'

    conn.execute(f"SET s3_endpoint = '{endpoint_without_protocol}';")
    conn.execute("SET s3_url_style = 'path';")
    conn.execute(f"SET s3_use_ssl = {use_ssl};")
    conn.execute(f"SET s3_access_key_id = '{MINIO_USER}';")
    conn.execute(f"SET s3_secret_access_key = '{MINIO_PASSWORD}';")

    return conn


def main():
    # Base local onde ficarão os arquivos DuckDB
    base_dir = Path(__file__).resolve().parent / "data"
    base_dir.mkdir(parents=True, exist_ok=True)

    # Caminho base dos Parquets (pasta GOLD no MinIO)
    gold_base = "s3://gold/sptrans"

    # Mapeia os datasets
    datasets = {
        "gold_frota_itaquera_positions": "frota_itaquera_positions",
        "gold_vel_media_5min_por_veiculo": "vel_media_5min_por_veiculo",
        "gold_vel_media_5min_por_linha": "vel_media_5min_por_linha",
        "gold_headway_aproximado": "headway_aproximado",
        "gold_densidade_grid": "densidade_grid",
        "gold_acessibilidade_share": "acessibilidade_share"
    }

    print("🚀 Iniciando geração de arquivos DuckDB individuais...")

    for folder, table_name in datasets.items():
        db_path = base_dir / f"{table_name}.duckdb"
        src_glob = f"{gold_base}/{folder}/*.parquet"

        print(f"\n📦 Criando {db_path.name} a partir de {src_glob} ...")

        conn = get_duckdb_connection(db_path)
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_parquet('{src_glob}');
        """)

        cnt = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"✅ Tabela '{table_name}' criada com {cnt} linhas.")
        conn.close()

    print("\n🎯 Todas as tabelas foram exportadas em arquivos separados em:")
    print(base_dir)


if __name__ == "__main__":
    main()
