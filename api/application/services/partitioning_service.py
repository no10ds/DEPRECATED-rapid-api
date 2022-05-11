from typing import List, Tuple

import pandas as pd

from api.domain.schema import Schema


def generate_path(group_partitions: List[str], group_info):
    formatted_group_partitions = [
        f"{partition}={value}" for partition, value in zip(group_partitions, group_info)
    ]
    return "/".join(formatted_group_partitions)


def drop_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    return df.drop(labels=columns, axis=1)


def generate_partitioned_data(
    schema: Schema, df: pd.DataFrame
) -> List[Tuple[str, pd.DataFrame]]:
    partitions = schema.get_partitions()

    if len(partitions) == 0:
        return non_partitioned_dataframe(df)
    return partitioned_dataframe(df, partitions)


def partitioned_dataframe(df, partitions) -> List[Tuple[str, pd.DataFrame]]:
    partitioned_data = []
    grouped = df.groupby(by=partitions)
    for group_spec, group_data in grouped:
        group_spec = (group_spec,) if len(partitions) == 1 else group_spec

        cleaned_dataframe = drop_columns(df=group_data, columns=partitions).reset_index(
            drop=True
        )

        partitioned_data.append(
            (generate_path(partitions, group_spec), cleaned_dataframe)
        )
    return partitioned_data


def non_partitioned_dataframe(df) -> List[Tuple[str, pd.DataFrame]]:
    return [("", df)]
