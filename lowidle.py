"""Low Idle Inspect"""

import os
import polars as pl

SCRIPT_VERSION = "V1.2"
COL_TIME = "Timestamp"
LOW_RPM = 1300


def calculate_id(df: pl.DataFrame) -> list[int]:
    """Função para criação de id"""
    ids = [0]
    current_id = 0
    for i in range(1, len(df)):
        if df[i, "RPM"] is not None:
            if df[i, "Is low rpm"] != df[i - 1, "Is low rpm"]:
                current_id += 1
            ids.append(current_id)
        else:
            current_id += 1
            ids.append(None)
    return ids


def data_extration(engdata_path: str) -> pl.DataFrame:
    """Abre arquivo e retorna dataframe"""
    df = pl.read_csv(
        engdata_path, infer_schema_length=0, columns=["Timestamp", "RPM", "Asset"]
    )
    df = df.with_columns(
        pl.coalesce(
            [
                pl.col("Timestamp").str.strptime(
                    pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False
                ),
                pl.col("Timestamp").str.strptime(
                    pl.Datetime, "%m/%d/%Y %H:%M", strict=False
                ),
                pl.col("Timestamp").str.strptime(
                    pl.Datetime, "%m/%d/%y %H:%M:%S", strict=False
                ),
                pl.col("Timestamp").str.strptime(
                    pl.Datetime, "%m/%d/%y %I:%M %p", strict=False
                ),
                pl.col("Timestamp").str.strptime(
                    pl.Datetime, "%m/%d/%Y %H:%M:%S", strict=False
                ),
                pl.col("Timestamp").str.strptime(
                    pl.Datetime, "%m/%d/%Y %I:%M:%S %p", strict=False
                ),
            ]
        ).alias("Timestamp")
    )
    df = df.cast({"RPM": pl.Float64})
    # df = df.drop_nulls(subset=["RPM"])
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


def clean_data(df: pl.DataFrame) -> pl.DataFrame:
    """Limpa dados com saltos de tempo maiores que 60 min"""
    counts = df.group_by("id").agg(pl.count("RPM").alias("count"))
    df = df.join(counts, on="id")
    df = df.filter(
        pl.when((df["count"] <= 1) & (df["Total time (min)"] > 60))
        .then(False)
        .otherwise(True)
    )
    return df


def main(path_db: str) -> None:
    """Função principal do Low Idle Inspect"""
    engdata_path = path_db + r"\history_output.csv"
    dfengdata = data_extration(engdata_path)
    listassets = extract_asset(dfengdata)

    dfengdata_calculated = pl.DataFrame()

    for asset in listassets:

        dfasset = dfengdata.filter(pl.col("Asset") == asset)
        dfengdata = dfengdata.filter(pl.col("Asset") != asset)

        dfasset = add_time_diff(dfasset, COL_TIME)
        dfasset = add_islowrpmcol(dfasset)
        dfasset = period_sum(dfasset)
        dfasset = clean_data(dfasset)

        dfengdata_calculated = pl.concat([dfengdata_calculated, dfasset])

    dfengdata_calculated.write_csv(
        path_db + r"\lowrpm_output.csv", datetime_format="%Y-%m-%d %H:%M:%S"
    )


if __name__ == "__main__":

    print("Execute o script através da GUI!")

    # Test Mode

    print("MODO DE TESTE!!!")

    path_engdata = os.getenv("PATH_BD")
    main(path_engdata)
