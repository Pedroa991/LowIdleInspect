"""Low Idle Inspect"""

import os
import polars as pl

SCRIPT_VERSION = "V1.0"
COL_TIME = "Timestamp"
LOW_RPM = 1300


def calculate_id(df: pl.DataFrame) -> list[int]:
    """Função para criação de id"""
    ids = [0]
    current_id = 0
    for i in range(1, len(df)):
        if df[i, "Is low rpm"] != df[i - 1, "Is low rpm"]:
            current_id += 1
        ids.append(current_id)
    return ids


def data_extration(engdata_path: str) -> pl.DataFrame:
    """Abre arquivo e retorna dataframe"""
    df = pl.read_csv(engdata_path, infer_schema_length=0)
    df = df.with_columns(
        pl.col(COL_TIME).str.strptime(pl.Datetime, format="%m/%d/%Y %H:%M")
    )
    df = df.cast({"RPM": pl.Float64})
    return df


def extract_asset(df: pl.DataFrame) -> list[str]:
    """Cria um set com todos serial numbers unicos dos motores"""
    listassets = df.select(pl.col("Asset")).to_series().unique().to_list()
    listassets.sort()
    return listassets


def add_time_diff(df: pl.DataFrame, colname: str) -> pl.DataFrame:
    """Adiciona a coluna com a diferença de tempo entre linhas"""
    df = df.sort(colname)
    df = df.with_columns(
        [
            (pl.col(colname).diff().dt.total_minutes().fill_null(0)).alias(
                "Time diff (min)"
            )
        ]
    )
    return df


def add_islowrpmcol(df: pl.DataFrame) -> pl.DataFrame:
    """Cria coluna condicional para mostrar se o valor está abaixo do RPM especificado"""
    df = df.with_columns(
        pl.when(pl.col("RPM") < 1300).then(0).otherwise(1).alias("Is low rpm")
    )
    return df


def period_sum(df: pl.DataFrame) -> pl.DataFrame:
    """Soma o tempo entre trechos"""
    df = df.with_columns(pl.Series("id", calculate_id(df)))
    sums = df.group_by("id").agg(pl.sum("Time diff (min)").alias("Total time (min)"))
    df = df.join(sums, on="id", how="left")
    return df


def main(path_db: str) -> None:
    """Função principal do Low Idle Inspect"""
    engdata_path = path_db + r"\history_output.csv"
    dfengdata = data_extration(engdata_path)
    listassets = extract_asset(dfengdata)

    dfengdata_calculeted = pl.DataFrame()

    for asset in listassets:

        dfasset = dfengdata.filter(pl.col("Asset") == asset)
        dfengdata = dfengdata.filter(pl.col("Asset") != asset)

        dfasset = add_time_diff(dfasset, COL_TIME)
        dfasset = add_islowrpmcol(dfasset)
        dfasset = period_sum(dfasset)

        dfengdata_calculated = pl.concat([dfengdata_calculeted, dfasset])

    dfengdata_calculated.write_csv("final.csv", datetime_format="%m/%d/%Y %H:%M")


if __name__ == "__main__":

    print("Execute o script através da GUI!")

    # Test Mode

    print("MODO DE TESTE!!!")

    path_engdata = os.getenv("PATH_BD") + r"\history_output.csv"
    main(path_engdata)
