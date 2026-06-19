"""Exporte le gold/marts de MinIO vers `include/snapshot/` (snapshot committé).

`minio-init` recharge ce snapshot dans le bucket au boot -> dashboards peuplés dès `docker compose up`,
sans rejouer le pipeline. Lancé par `scripts/make_snapshot.sh` (dans le conteneur Airflow).

Tables exportées (1 fichier par table) :
- marts servis au front : mart_commune, mart_commune_type, mart_opportunites, dvf_stats_commune_type
- fact_biens : dernière partition `dt` uniquement (grain DPE, pour la répartition A→G)
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, "/opt/airflow/include")
from duckdb_lake import connect  # noqa: E402

BUCKET = os.getenv("MINIO_BUCKET", "immolake")
SNAP_DIR = "/opt/airflow/include/snapshot/gold"
# fact_biens peut peser plusieurs M de lignes (ingestion complète) : on échantillonne pour garder
# un snapshot léger et committable. La répartition A→G reste représentative ; les marts (servis au
# front) sont eux exportés en intégralité.
FACT_SAMPLE = int(os.getenv("SNAPSHOT_FACT_LIMIT", "500000"))

# table -> (source glob s3, filtre éventuel)
MARTS = ("mart_commune", "mart_commune_type", "mart_opportunites", "dvf_stats_commune_type")


def main() -> None:
    con = connect()
    os.makedirs(SNAP_DIR, exist_ok=True)

    for name in MARTS:
        src = f"s3://{BUCKET}/gold/{name}/**/*.parquet"
        out_dir = f"{SNAP_DIR}/{name}"
        os.makedirs(out_dir, exist_ok=True)
        n = con.execute(f"SELECT count(*) FROM read_parquet('{src}')").fetchone()[0]
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{src}')) "
            f"TO '{out_dir}/data.parquet' (FORMAT PARQUET, COMPRESSION zstd)"
        )
        print(f"{name}: {n} lignes -> include/snapshot/gold/{name}/data.parquet")

    # fact_biens : dernière partition dt, échantillonnée (snapshot léger ; répartition A→G conservée)
    fact_src = f"s3://{BUCKET}/gold/fact_biens/**/*.parquet"
    out_dir = f"{SNAP_DIR}/fact_biens"
    os.makedirs(out_dir, exist_ok=True)
    latest = (
        f"SELECT * FROM read_parquet('{fact_src}') "
        f"WHERE dt = (SELECT max(dt) FROM read_parquet('{fact_src}'))"
    )
    total = con.execute(f"SELECT count(*) FROM ({latest})").fetchone()[0]
    con.execute(
        f"COPY (SELECT * FROM ({latest}) USING SAMPLE {FACT_SAMPLE} ROWS) "
        f"TO '{out_dir}/data.parquet' (FORMAT PARQUET, COMPRESSION zstd)"
    )
    kept = min(total, FACT_SAMPLE)
    print(f"fact_biens: {kept}/{total} lignes (échantillon) -> include/snapshot/gold/fact_biens/data.parquet")
    con.close()


if __name__ == "__main__":
    main()
