"""Transformation raw DPE -> silver Parquet -> gold facts."""
from __future__ import annotations

import json
import logging
import os
from io import BytesIO
from typing import Any

import pandas as pd
import pendulum
import requests
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "immolake")
RAW_PREFIX = "raw/dpe"
RAW_DVF_PREFIX = "raw/dvf"
SILVER_PREFIX = "silver/dpe"
SILVER_DVF_PREFIX = "silver/dvf"
GOLD_PREFIX = "gold/fact_biens"
GOLD_KPI_PREFIX = "gold/kpi_commune"
LOGGER = logging.getLogger(__name__)

SILVER_COLUMNS = [
    "numero_dpe",
    "dt",
    "code_insee",
    "code_postal",
    "type_batiment",
    "surface_habitable",
    "etiquette_dpe",
    "conso_energie",
    "date_etablissement",
]

CANONICAL_COLUMNS = {
    "numero_dpe": ("numero_dpe",),
    "code_insee": ("code_insee", "code_insee_ban"),
    "type_batiment": ("type_batiment", "type_bien", "type"),
    "surface": ("surface_habitable", "surface", "surface_habitable_logement"),
    "etiquette": ("etiquette_dpe", "etiquette", "classe_dpe"),
    "conso_energie": (
        "conso_energie",
        "conso_5_usages_ep_m2",
        "consommation_energie",
        "consommation_energie_finale",
    ),
}

DVF_COLUMNS = {
    "code_insee": ("code_insee", "code_commune", "code_insee_commune"),
    "type_bien": ("type_bien", "type_local", "type_batiment"),
    "surface": ("surface", "surface_reelle_bati", "surface_habitable"),
    "prix": ("prix", "valeur_fonciere", "valeur_fonciere_euros"),
    "prix_m2": ("prix_m2", "prix_m2_median"),
    "date_mutation": ("date_mutation", "date", "date_transaction"),
}

SILVER_DVF_COLUMNS = ["code_insee", "type_bien", "surface", "prix", "prix_m2", "date_mutation"]


def _ds(ds: str | None) -> str:
    if ds:
        return ds
    return get_current_context()["ds"]


def _first_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _clean_dpe_rows(rows: list[dict[str, Any]], run_ds: str) -> pd.DataFrame:
    records = []
    for row in rows:
        records.append(
            {
                "numero_dpe": row.get("numero_dpe"),
                "dt": run_ds,
                "code_insee": _first_value(row, "code_insee_ban", "code_insee"),
                "code_postal": _first_value(row, "code_postal_ban", "code_postal_brut", "code_postal"),
                "type_batiment": row.get("type_batiment"),
                "surface_habitable": _first_value(
                    row,
                    "surface_habitable_logement",
                    "surface_habitable_immeuble",
                ),
                "etiquette_dpe": row.get("etiquette_dpe"),
                "conso_energie": _first_value(row, "conso_5_usages_par_m2_ep", "conso_5_usages_ep"),
                "date_etablissement": row.get("date_etablissement_dpe"),
            }
        )

    df = pd.DataFrame.from_records(records, columns=SILVER_COLUMNS)
    if df.empty:
        return df.astype(
            {
                "numero_dpe": "string",
                "dt": "string",
                "code_insee": "string",
                "code_postal": "string",
                "type_batiment": "string",
                "etiquette_dpe": "string",
            }
        )

    df["numero_dpe"] = df["numero_dpe"].astype("string").str.strip()
    df["dt"] = df["dt"].astype("string")
    df["code_insee"] = df["code_insee"].astype("string").str.strip()
    df["code_postal"] = df["code_postal"].astype("string").str.replace(r"\.0$", "", regex=True).str.strip()
    df["type_batiment"] = df["type_batiment"].astype("string").str.strip().str.lower()
    df["surface_habitable"] = pd.to_numeric(
        df["surface_habitable"].astype("string").str.replace(",", ".", regex=False),
        errors="coerce",
    )
    df["etiquette_dpe"] = df["etiquette_dpe"].astype("string").str.strip().str.upper()
    df["conso_energie"] = pd.to_numeric(
        df["conso_energie"].astype("string").str.replace(",", ".", regex=False),
        errors="coerce",
    )
    df["date_etablissement"] = pd.to_datetime(df["date_etablissement"], errors="coerce").dt.date

    df = df.dropna(subset=["numero_dpe", "code_insee", "etiquette_dpe"])
    df = df[df["surface_habitable"].isna() | (df["surface_habitable"] > 0)]
    df = df.drop_duplicates(subset=["numero_dpe"], keep="last")
    return df.reset_index(drop=True)


def _to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    return buffer.getvalue()


def _read_parquet_partition(s3_hook: S3Hook, prefix: str) -> pd.DataFrame:
    keys = [
        key
        for key in s3_hook.list_keys(bucket_name=MINIO_BUCKET, prefix=prefix) or []
        if key.endswith(".parquet")
    ]
    if not keys:
        return pd.DataFrame()

    s3_client = s3_hook.get_conn()
    frames = []
    for key in sorted(keys):
        obj = s3_client.get_object(Bucket=MINIO_BUCKET, Key=key)
        frames.append(pd.read_parquet(BytesIO(obj["Body"].read())))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _read_silver_partition(s3_hook: S3Hook, run_ds: str) -> pd.DataFrame:
    prefix = f"{SILVER_PREFIX}/dt={run_ds}/"
    df = _read_parquet_partition(s3_hook, prefix)
    if df.empty:
        raise AirflowException(f"Aucun fichier Parquet trouve dans s3://{MINIO_BUCKET}/{prefix}")
    return df


def _read_dvf_partition(s3_hook: S3Hook, run_ds: str) -> pd.DataFrame:
    return _read_parquet_partition(s3_hook, f"{SILVER_DVF_PREFIX}/dt={run_ds}/")


def _first_existing_column(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    lower_to_original = {column.lower(): column for column in df.columns}
    for candidate in candidates:
        column = lower_to_original.get(candidate.lower())
        if column:
            return column
    return None


def _select_column(df: pd.DataFrame, output_name: str) -> pd.Series:
    source_name = _first_existing_column(df, CANONICAL_COLUMNS[output_name])
    if source_name is None:
        if output_name == "numero_dpe":
            return pd.Series(pd.NA, index=df.index, dtype="string")
        raise AirflowException(
            f"Colonne silver manquante pour {output_name}. "
            f"Candidats attendus: {', '.join(CANONICAL_COLUMNS[output_name])}"
        )
    return df[source_name]


def _select_optional_column(
    df: pd.DataFrame,
    candidates: tuple[str, ...],
    dtype: str = "object",
) -> pd.Series:
    source_name = _first_existing_column(df, candidates)
    if source_name is None:
        return pd.Series(pd.NA, index=df.index, dtype=dtype)
    return df[source_name]


def _to_numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype("string")
        .str.replace("\u00a0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False),
        errors="coerce",
    )


def _normalize_type_bien(value: object) -> str:
    text = str(value or "").strip().lower()
    if "appart" in text:
        return "appartement"
    if "maison" in text:
        return "maison"
    return "autre"


def _clean_dvf_frame(dvf_df: pd.DataFrame) -> pd.DataFrame:
    if dvf_df.empty:
        return pd.DataFrame(columns=SILVER_DVF_COLUMNS)

    cleaned = pd.DataFrame(
        {
            "code_insee": _select_optional_column(dvf_df, DVF_COLUMNS["code_insee"], dtype="string")
            .astype("string")
            .str.replace(r"\.0$", "", regex=True)
            .str.zfill(5)
            .str.strip(),
            "type_bien": _select_optional_column(dvf_df, DVF_COLUMNS["type_bien"]).map(_normalize_type_bien),
            "surface": _to_numeric_series(_select_optional_column(dvf_df, DVF_COLUMNS["surface"])),
            "prix": _to_numeric_series(_select_optional_column(dvf_df, DVF_COLUMNS["prix"])),
            "prix_m2": _to_numeric_series(_select_optional_column(dvf_df, DVF_COLUMNS["prix_m2"])),
            "date_mutation": pd.to_datetime(
                _select_optional_column(dvf_df, DVF_COLUMNS["date_mutation"]),
                errors="coerce",
                dayfirst=True,
            ).dt.date,
        }
    )
    missing_prix_m2 = cleaned["prix_m2"].isna()
    cleaned.loc[missing_prix_m2, "prix_m2"] = cleaned.loc[missing_prix_m2, "prix"] / cleaned.loc[
        missing_prix_m2,
        "surface",
    ]
    cleaned = cleaned[
        cleaned["code_insee"].notna()
        & (cleaned["code_insee"] != "")
        & cleaned["surface"].notna()
        & (cleaned["surface"] > 0)
        & cleaned["prix"].notna()
        & (cleaned["prix"] > 0)
        & cleaned["prix_m2"].notna()
        & (cleaned["prix_m2"] > 0)
    ]
    return cleaned[SILVER_DVF_COLUMNS].reset_index(drop=True)


def _prepare_fact_frame(silver_df: pd.DataFrame, run_ds: str) -> pd.DataFrame:
    if silver_df.empty:
        raise AirflowException("La partition silver est vide, fact_biens ne peut pas etre construit")

    fact_df = pd.DataFrame(
        {
            "numero_dpe": _select_column(silver_df, "numero_dpe").astype("string").str.strip(),
            "dt": pd.to_datetime(run_ds).date(),
            "code_insee": _select_column(silver_df, "code_insee").astype("string").str.strip(),
            "etiquette": _select_column(silver_df, "etiquette")
            .astype("string")
            .str.strip()
            .str.upper(),
            "type_bien": _select_column(silver_df, "type_batiment").map(_normalize_type_bien),
            "surface": pd.to_numeric(_select_column(silver_df, "surface"), errors="coerce"),
            "conso_energie": pd.to_numeric(_select_column(silver_df, "conso_energie"), errors="coerce"),
        }
    )

    fact_df = fact_df[
        fact_df["code_insee"].notna()
        & (fact_df["code_insee"] != "")
        & fact_df["surface"].notna()
        & (fact_df["surface"] > 0)
    ].copy()
    fact_df["numero_dpe"] = fact_df["numero_dpe"].replace("", pd.NA)

    if fact_df["numero_dpe"].notna().any():
        with_dpe_id = fact_df[fact_df["numero_dpe"].notna()]
        without_dpe_id = fact_df[fact_df["numero_dpe"].isna()]
        fact_df = pd.concat(
            [with_dpe_id.drop_duplicates(subset=["numero_dpe"], keep="last"), without_dpe_id],
            ignore_index=True,
        )

    fact_df["prix"] = pd.Series([pd.NA] * len(fact_df), dtype="Float64")
    fact_df["prix_m2"] = pd.Series([pd.NA] * len(fact_df), dtype="Float64")
    return fact_df


def _prepare_dvf_price_reference(dvf_df: pd.DataFrame) -> pd.DataFrame:
    if dvf_df.empty:
        return pd.DataFrame(columns=["code_insee", "type_bien", "dvf_prix_m2", "dvf_nb_transactions"])

    price_df = pd.DataFrame(
        {
            "code_insee": _select_optional_column(dvf_df, DVF_COLUMNS["code_insee"], dtype="string")
            .astype("string")
            .str.strip(),
            "type_bien": _select_optional_column(dvf_df, DVF_COLUMNS["type_bien"]).map(_normalize_type_bien),
            "surface": _to_numeric_series(_select_optional_column(dvf_df, DVF_COLUMNS["surface"])),
            "prix": _to_numeric_series(_select_optional_column(dvf_df, DVF_COLUMNS["prix"])),
            "prix_m2": _to_numeric_series(_select_optional_column(dvf_df, DVF_COLUMNS["prix_m2"])),
        }
    )
    missing_prix_m2 = price_df["prix_m2"].isna()
    price_df.loc[missing_prix_m2, "prix_m2"] = price_df.loc[missing_prix_m2, "prix"] / price_df.loc[
        missing_prix_m2,
        "surface",
    ]
    price_df = price_df[
        price_df["code_insee"].notna()
        & (price_df["code_insee"] != "")
        & price_df["prix_m2"].notna()
        & (price_df["prix_m2"] > 0)
    ].copy()

    if price_df.empty:
        return pd.DataFrame(columns=["code_insee", "type_bien", "dvf_prix_m2", "dvf_nb_transactions"])

    return (
        price_df.groupby(["code_insee", "type_bien"], as_index=False)
        .agg(dvf_prix_m2=("prix_m2", "median"), dvf_nb_transactions=("prix_m2", "size"))
    )


def _enrich_prices_from_dvf(fact_df: pd.DataFrame, dvf_prices: pd.DataFrame) -> pd.DataFrame:
    if dvf_prices.empty:
        LOGGER.warning("Aucune donnee DVF exploitable: prix et prix_m2 restent NULL")
        return fact_df

    enriched = fact_df.merge(dvf_prices, on=["code_insee", "type_bien"], how="left")
    matched_mask = enriched["dvf_prix_m2"].notna()
    enriched.loc[matched_mask, "prix_m2"] = enriched.loc[matched_mask, "dvf_prix_m2"]
    enriched.loc[matched_mask, "prix"] = enriched.loc[matched_mask, "surface"] * enriched.loc[matched_mask, "prix_m2"]
    LOGGER.info(
        "Enrichissement DVF: %s/%s lignes enrichies, %s sans prix",
        int(matched_mask.sum()),
        len(enriched),
        int((~matched_mask).sum()),
    )
    return enriched.drop(columns=["dvf_prix_m2", "dvf_nb_transactions"])


def _attach_dimensions(fact_df: pd.DataFrame, postgres_hook: PostgresHook) -> pd.DataFrame:
    dim_commune = postgres_hook.get_pandas_df("SELECT code_insee FROM dwh.dim_commune")
    dim_dpe = postgres_hook.get_pandas_df("SELECT etiquette FROM dwh.dim_dpe")
    dim_type_bien = postgres_hook.get_pandas_df("SELECT id AS type_bien_id, type FROM dwh.dim_type_bien")

    fact_df = fact_df.merge(dim_commune, on="code_insee", how="left", indicator="commune_match")
    missing_mask = fact_df["commune_match"] == "left_only"
    missing_communes = sorted(fact_df.loc[missing_mask, "code_insee"].unique())
    if missing_communes:
        sample = ", ".join(missing_communes[:20])
        get_current_context()["ti"].log.warning(
            "%s lignes avec code_insee absent de dwh.dim_commune (%s codes distincts). Exemples: %s",
            int(missing_mask.sum()),
            len(missing_communes),
            sample,
        )
    fact_df = fact_df[fact_df["commune_match"] == "both"].drop(columns=["commune_match"])

    fact_df = fact_df.merge(dim_dpe, on="etiquette", how="left", indicator="dpe_match")
    invalid_dpe = sorted(fact_df.loc[fact_df["dpe_match"] == "left_only", "etiquette"].dropna().unique())
    if invalid_dpe:
        get_current_context()["ti"].log.warning("Etiquettes DPE ignorees: %s", ", ".join(invalid_dpe))
    fact_df = fact_df[fact_df["dpe_match"] == "both"].drop(columns=["dpe_match"])

    fact_df = fact_df.merge(dim_type_bien, left_on="type_bien", right_on="type", how="left")
    fact_df["type_bien_id"] = fact_df["type_bien_id"].astype("Int64")
    fact_df = fact_df.drop(columns=["numero_dpe", "type_bien", "type"])
    return fact_df[
        [
            "dt",
            "code_insee",
            "etiquette",
            "type_bien_id",
            "surface",
            "prix",
            "prix_m2",
            "conso_energie",
        ]
    ]


def _write_gold_partition(s3_hook: S3Hook, fact_df: pd.DataFrame, run_ds: str) -> str:
    if fact_df.empty:
        raise AirflowException("Aucune ligne valide apres rattachement des dimensions")

    prefix = f"{GOLD_PREFIX}/dt={run_ds}/"
    existing_keys = s3_hook.list_keys(bucket_name=MINIO_BUCKET, prefix=prefix) or []
    if existing_keys:
        s3_hook.delete_objects(bucket=MINIO_BUCKET, keys=existing_keys)

    output_key = f"{prefix}fact_biens.parquet"
    s3_hook.load_bytes(
        bytes_data=_to_parquet_bytes(fact_df),
        key=output_key,
        bucket_name=MINIO_BUCKET,
        replace=True,
    )
    return output_key


def _build_kpi_commune(fact_df: pd.DataFrame, dim_dpe_df: pd.DataFrame, run_ds: str) -> pd.DataFrame:
    """Agrege le gold fact_biens en KPIs par commune (prix/m2 median, passoires, decote)."""
    if fact_df.empty:
        raise AirflowException("Le gold fact_biens est vide, kpi_commune ne peut pas etre construit")

    df = fact_df.copy()
    df["prix_m2"] = pd.to_numeric(df["prix_m2"], errors="coerce")
    df = df.merge(dim_dpe_df, on="etiquette", how="left")
    df["is_passoire"] = df["label_passoire"].astype("boolean").fillna(False)

    agg = df.groupby("code_insee").agg(
        prix_m2_median=("prix_m2", "median"),
        nb_transactions=("code_insee", "size"),
        pct_passoires=("is_passoire", lambda s: round(100.0 * float(s.mean()), 2)),
    )
    pass_m2 = df[df["is_passoire"]].groupby("code_insee")["prix_m2"].mean()
    nonpass_m2 = df[~df["is_passoire"]].groupby("code_insee")["prix_m2"].mean()
    agg = agg.join(pass_m2.rename("_pass")).join(nonpass_m2.rename("_nonpass"))
    base = agg["_nonpass"].replace(0, pd.NA)
    agg["decote_passoire_pct"] = (100.0 * (agg["_pass"] - agg["_nonpass"]) / base).round(2)

    agg = agg.reset_index()
    agg["dt"] = pd.to_datetime(run_ds).date()
    agg["nb_transactions"] = agg["nb_transactions"].astype("int64")
    return agg[
        [
            "dt",
            "code_insee",
            "prix_m2_median",
            "pct_passoires",
            "decote_passoire_pct",
            "nb_transactions",
        ]
    ]


def _write_gold_kpi_partition(s3_hook: S3Hook, kpi_df: pd.DataFrame, run_ds: str) -> str:
    prefix = f"{GOLD_KPI_PREFIX}/dt={run_ds}/"
    existing_keys = s3_hook.list_keys(bucket_name=MINIO_BUCKET, prefix=prefix) or []
    if existing_keys:
        s3_hook.delete_objects(bucket=MINIO_BUCKET, keys=existing_keys)
    output_key = f"{prefix}kpi_commune.parquet"
    s3_hook.load_bytes(
        bytes_data=_to_parquet_bytes(kpi_df),
        key=output_key,
        bucket_name=MINIO_BUCKET,
        replace=True,
    )
    return output_key


@dag(
    dag_id="immolake_transform_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    catchup=False,
    tags=["immolake", "transform"],
)
def immolake_transform_daily():
    @task
    def raw_to_silver(ds: str | None = None) -> str:
        run_ds = _ds(ds)
        raw_key = f"{RAW_PREFIX}/dt={run_ds}/data.json"
        silver_key = f"{SILVER_PREFIX}/dt={run_ds}/data.parquet"

        s3 = S3Hook(aws_conn_id="minio_default")
        payload = json.loads(s3.read_key(key=raw_key, bucket_name=MINIO_BUCKET))
        df = _clean_dpe_rows(payload.get("results", []), run_ds)
        s3.load_bytes(
            bytes_data=_to_parquet_bytes(df),
            key=silver_key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )
        return silver_key

    @task
    def dvf_to_raw(ds: str | None = None) -> str:
        run_ds = _ds(ds)
        raw_key = f"{RAW_DVF_PREFIX}/dt={run_ds}/data.csv"
        dvf_csv_url = os.getenv("DVF_CSV_URL")
        if not dvf_csv_url:
            raise AirflowException("DVF_CSV_URL doit etre renseigne pour produire raw/dvf")

        response = requests.get(dvf_csv_url, timeout=120)
        response.raise_for_status()

        s3 = S3Hook(aws_conn_id="minio_default")
        s3.load_bytes(
            bytes_data=response.content,
            key=raw_key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )
        return raw_key

    @task
    def raw_dvf_to_silver(ds: str | None = None) -> str:
        run_ds = _ds(ds)
        raw_key = f"{RAW_DVF_PREFIX}/dt={run_ds}/data.csv"
        silver_key = f"{SILVER_DVF_PREFIX}/dt={run_ds}/data.parquet"
        max_rows = int(os.getenv("DVF_MAX_ROWS", "0") or "0") or None

        s3 = S3Hook(aws_conn_id="minio_default")
        raw_obj = s3.get_conn().get_object(Bucket=MINIO_BUCKET, Key=raw_key)
        raw_bytes = raw_obj["Body"].read()
        raw_df = pd.read_csv(BytesIO(raw_bytes), sep=None, engine="python", nrows=max_rows)
        silver_df = _clean_dvf_frame(raw_df)
        s3.load_bytes(
            bytes_data=_to_parquet_bytes(silver_df),
            key=silver_key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )
        LOGGER.info("Ecriture silver DVF terminee: s3://%s/%s (%s lignes)", MINIO_BUCKET, silver_key, len(silver_df))
        return silver_key

    @task
    def refresh_dimensions() -> None:
        """Dimensions are seeded at postgres-dwh init time for the MVP."""
        PostgresHook(postgres_conn_id="dwh_postgres").get_first("SELECT 1 FROM dwh.dim_commune LIMIT 1")

    @task
    def load_fact_biens(ds: str | None = None) -> None:
        run_ds = _ds(ds)
        s3_hook = S3Hook(aws_conn_id="minio_default")
        postgres_hook = PostgresHook(postgres_conn_id="dwh_postgres")

        silver_df = _read_silver_partition(s3_hook, run_ds)
        dvf_df = _read_dvf_partition(s3_hook, run_ds)
        fact_df = _prepare_fact_frame(silver_df, run_ds)
        fact_df = _enrich_prices_from_dvf(fact_df, _prepare_dvf_price_reference(dvf_df))
        fact_df = _attach_dimensions(fact_df, postgres_hook)
        output_key = _write_gold_partition(s3_hook, fact_df, run_ds)

        get_current_context()["ti"].log.info(
            "Ecriture gold fact_biens terminee: s3://%s/%s (%s lignes)",
            MINIO_BUCKET,
            output_key,
            len(fact_df),
        )

    @task
    def build_kpi_commune(ds: str | None = None) -> None:
        run_ds = _ds(ds)
        s3_hook = S3Hook(aws_conn_id="minio_default")
        postgres_hook = PostgresHook(postgres_conn_id="dwh_postgres")

        fact_df = _read_parquet_partition(s3_hook, f"{GOLD_PREFIX}/dt={run_ds}/")
        if fact_df.empty:
            raise AirflowException(f"Gold fact_biens introuvable pour dt={run_ds}")
        dim_dpe = postgres_hook.get_pandas_df("SELECT etiquette, label_passoire FROM dwh.dim_dpe")
        kpi_df = _build_kpi_commune(fact_df, dim_dpe, run_ds)
        output_key = _write_gold_kpi_partition(s3_hook, kpi_df, run_ds)

        get_current_context()["ti"].log.info(
            "Ecriture gold kpi_commune terminee: s3://%s/%s (%s communes)",
            MINIO_BUCKET,
            output_key,
            len(kpi_df),
        )

    dpe_silver = raw_to_silver()
    dvf_silver = raw_dvf_to_silver()
    dvf_to_raw() >> dvf_silver
    fact = load_fact_biens()
    [dpe_silver, dvf_silver, refresh_dimensions()] >> fact >> build_kpi_commune()


immolake_transform_daily()
