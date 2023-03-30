import os
import re
import psutil
from typing import List, Any
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import UploadFile, File
from pandas.io.parsers import TextFileReader

from api.common.logger import AppLogger
from api.common.config.constants import (
    CHUNK_SIZE_MB,
    PARQUET_CHUNK_SIZE,
    CONTENT_ENCODING,
)
from api.domain.schema import Schema

CHUNK_SIZE = 200_000


def parse_categorisation(
    path: str, categories: List[str], category_name: str = "categorisation"
) -> str:
    match = re.findall(rf"({'|'.join(categories)})", path)
    if match:
        return match[0]
    else:
        raise ValueError(f"Could not find {category_name}")


def store_file_to_disk(
    extension: str, id: str, file: UploadFile = File(...), to_chunk: bool = False
) -> Path:
    file_path = Path(f"{id}-{file.filename}")
    AppLogger.info(
        f"Writing incoming file chunk ({CHUNK_SIZE_MB}MB) to disk [{file.filename}]"
    )
    AppLogger.info(f"Available disk space: {psutil.disk_usage('/').free / (2 ** 30)}GB")

    if extension == "csv":
        store_csv_file_to_disk(file_path, to_chunk, file)
    elif extension == "parquet":
        store_parquet_file_to_disk(file_path, to_chunk, file)
    return file_path


def store_csv_file_to_disk(
    file_path: Path, to_chunk: bool, file: UploadFile = File(...)
):
    with open(file_path, "wb") as incoming_file:
        while contents := file.file.read(CHUNK_SIZE_MB):
            incoming_file.write(contents)

            if to_chunk:
                incoming_file.close()
                break


def store_parquet_file_to_disk(
    file_path: Path, to_chunk: bool, file: UploadFile = File(...)
):
    parquet_file = pq.ParquetFile(file.file)
    for index, batch in enumerate(parquet_file.iter_batches(PARQUET_CHUNK_SIZE)):
        if index == 0:
            writer = pq.ParquetWriter(file_path.as_posix(), batch.schema)

        table = pa.Table.from_batches([batch])
        writer.write_table(table)

        if to_chunk:
            break
    writer.close()


def construct_chunked_dataframe(
    file_path: Path,
) -> TextFileReader | Any:
    # Loads the file from the local path and splits into each dataframe chunk for processing
    # when loading csv Pandas returns an IO iterable TextFileReader but for a Pyarrow chunking
    # it retuns an iterable of pyarrow.RecordBatch
    extension = file_path.as_posix().split(".")[-1].lower()
    if extension == "csv":
        return pd.read_csv(
            file_path, encoding=CONTENT_ENCODING, sep=",", chunksize=CHUNK_SIZE
        )
    elif extension == "parquet":
        parquet_file = pq.ParquetFile(file_path.as_posix())
        return parquet_file.iter_batches(batch_size=CHUNK_SIZE)


def delete_incoming_raw_file(
    schema: Schema, file_path: Path, raw_file_identifier: str = None
):
    raw_file_identifier_string = f"Raw file identifier: {raw_file_identifier}"
    try:
        os.remove(file_path.name)
        AppLogger.info(
            f"""Temporary upload file for {schema.get_domain()}/{schema.get_dataset()}/{schema.get_version()} deleted. {raw_file_identifier_string if raw_file_identifier is not None else ''}"""
        )
    except (FileNotFoundError, TypeError) as error:
        AppLogger.error(
            f"Temporary upload file for {schema.get_domain()}/{schema.get_dataset()}/{schema.get_version()} not deleted. {raw_file_identifier_string if raw_file_identifier is not None else ''}. Detail: {error}"
        )
