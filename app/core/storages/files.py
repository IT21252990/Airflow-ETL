import os
import duckdb

import pyarrow.csv as pc
import pyarrow.dataset as ds
import polars as pl
from pyarrow import Table

from datetime import (
    datetime,
    timezone
)

from fsspec.implementations.local import LocalFileSystem
from fsspec import AbstractFileSystem
from typing import Literal

from .base import BaseStorage


class FileStorage(BaseStorage):
    TYPE = 'FILE_STORAGE'

    def __init__(self, config_dict):
        super().__init__(config_dict)
        self.storage_backend: Literal['fs', 's3', 'gcs', 'abs'] = config_dict['storage_backend']
        self.path = config_dict['path']
        self.key = config_dict['key']
        self.use_package: Literal['arrow', 'arrow_ds', 'pandas', 'polars', 'duckdb'] = self.config_dict['use_package']


class CsvStorage(FileStorage):
    TYPE = 'CSV_FILE_STORAGE'

    def __init__(self, config_dict):
        super().__init__(config_dict)
        self.time_fmt = config_dict.get('time_fmt', '%Y-%m-%dT%H:%M:%S.%f%z')

    def __use_arrow_ds(self, fs: AbstractFileSystem, file_path: str, data: Table):
        ds.write_dataset(
            data=data,
            base_dir=file_path,
            format='csv',
            filesystem=fs
        )

    def __use_arrow(self, file_path: str, data: Table):
        pc.write_csv(data, file_path)

    def __use_pandas(self, file_path: str, data: Table):
        df = data.to_pandas()
        df.to_csv(file_path)

    def __use_polars(self, file_path: str, data: Table):
        df = pl.from_arrow(data)
        df.write_csv(file_path)

    def __use_duckdb(self, file_path: str, data: Table):
        duckdb.sql('select * from data').write_csv(file_path)

    def _write_csv(self, fs: AbstractFileSystem, data: Table):
        self.logger.info(f"store files using 'package' = {self.use_package}")

        package_mgr: dict = {
            'arrow': self.__use_arrow,
            'pandas': self.__use_pandas,
            'polars': self.__use_polars,
            'duckdb': self.__use_duckdb
        }

        file_path = os.path.join(self.path, f'{self.key}-{datetime.now(timezone.utc).strftime(self.time_fmt)}')

        if self.storage_backend != 'fs' or self.use_package == 'arrow_ds':
            return self.__use_arrow_ds(fs, file_path, data)
        else:
            fs_writer = package_mgr[self.use_package]
            return fs_writer(f'{file_path}.csv', data)

    def do_store(self, data):
        if self.storage_backend == 'fs':
            self.logger.info(f"store files using 'storage_backend' = [{self.storage_backend}]")
            fs = LocalFileSystem(auto_mkdir=True)
            self._write_csv(fs, data)
            return None
        else:
            raise NotImplementedError()
