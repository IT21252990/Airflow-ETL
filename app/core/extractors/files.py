import os

import duckdb
import pyarrow as pa
import pyarrow.csv as pc
import pandas as pd
import polars as pl
import pyarrow.dataset as ds
from pyarrow import Table

from fsspec.implementations.local import LocalFileSystem
from fsspec import AbstractFileSystem
from typing import Literal

from .base import BaseExtractor

from ...schemas import FileFilterParams


class FileExtractor(BaseExtractor):
    TYPE = 'FILE_EXTRACTOR'
    __HK_FILE = 'etl.housekeeping'

    def __init__(self, config_dict, transformer):
        super().__init__(config_dict, transformer)
        self.storage_backend: Literal['fs', 's3', 'gcs', 'abs'] = config_dict['storage_backend']
        self.path = config_dict['path']
        self.hk_file = self.config_dict.get('housekeeping', self.__HK_FILE)
        self.reprocess = self.config_dict.get('reprocess', False)
        self.fail_no_files = self.config_dict.get('fail_no_files', False)
        self.use_package: Literal['arrow', 'arrow_ds', 'pandas', 'polars', 'duckdb'] = self.config_dict['use_package']
        self.filters = self.config_dict.get('filters', {})

    def _filter_files(self, files: list, file_ext: str):
        filters = {} if self.filters is None else self.filters
        filters = FileFilterParams.model_validate(filters)

        self.logger.info(f"filter 'file_ext': {file_ext}")
        files = [file for file in files if file.split('.')[-1] == file_ext]
        self.logger.info(f"keep {len(files)} file(s) with 'file_ext': {file_ext}")

        if filters.key is not None and filters.key.strip() != '':
            self.logger.info("ignore filter options 'include' and 'exclude'")
            self.logger.info(f"filter 'key': {filters.key}")

            files = [file for file in files if file.split('/')[-1].split('.')[0] == filters.key]
            self.logger.info(f"keep {len(files)} file(s) with 'key': {filters.key}")
        else:
            if bool(filters.include):
                self.logger.info(f"filter 'include': {filters.include}")

                files = [file for file in files for key in filters.include
                         if key in file.split('/')[-1].split('.')[0]]

                self.logger.info(f"keep {len(files)} file(s) with 'include': {filters.include}")

            if bool(filters.exclude):
                self.logger.info(f"filter 'exclude': {filters.exclude}")

                files = [file for file in files for key in filters.exclude
                         if key not in file.split('/')[-1].split('.')[0]]

                self.logger.info(f"keep {len(files)} file(s) with 'exclude': {filters.exclude}")

        if len(files) > 0:
            if bool(filters.skip):
                self.logger.info(f"skip {filters.skip} file(s)")

                files = files[filters.skip::]

                self.logger.info(f"keep {len(files)} file(s) with 'skip': {filters.skip}")

            if bool(filters.keep_latest):
                files = [files[0]]
                self.logger.info(f"keep {len(files)} file, the latest {files}")

        return files

    def _get_hk_data(self, fs: AbstractFileSystem) -> list:
        processed_files = []
        hk_file_path = os.path.join(self.path, self.hk_file)

        if fs.exists(hk_file_path):
            self.logger.info(f'read {self.hk_file} file')
            processed_files = fs.read_text(
                path=hk_file_path,
                encoding='utf-8'
            ).splitlines()
        else:
            self.logger.warning(f'{self.hk_file} not available')

        return processed_files

    def _update_hf_file(self, fs: AbstractFileSystem, files: list[str]):
        hk_file_path = os.path.join(self.path, self.hk_file)

        hk_files = self._get_hk_data(fs)
        files = [file.split('/')[-1] for file in files]

        hk_data = set(hk_files).union(set(files))
        hk_data = '\n'.join(hk_data) + '\n'

        fs.write_text(
            path=hk_file_path,
            value=hk_data,
            encoding='utf-8'
        )

    def _list_files(self, fs: AbstractFileSystem, file_ext: str):
        files = fs.ls(self.path, detail=True)
        files = sorted(files, key=lambda x: x['mtime'], reverse=True)

        # ignore directories and .housekeeping files
        files = [file['name'] for file in files if file['type'] == 'file' and file['name'] != self.hk_file]
        self.logger.info(f'retrieved {len(files)} file(s) from {self.path}')

        files = self._filter_files(files, file_ext)

        if self.reprocess:
            self.logger.info(f'ignore {self.hk_file} file')
            self.logger.info(f'(re)-extracting all {len(files)} file(s) from {self.path}')
        else:
            processed_files = self._get_hk_data(fs)
            processed_files = [os.path.join(self.path, file) for file in processed_files]

            files = list(set(files) - set(processed_files))
            self.logger.info(f'extract {len(files)} file(s) after housekeeping')

        if not bool(files) and self.fail_no_files:
            raise FileNotFoundError()

        return files


class CsvFileExtractor(FileExtractor):
    TYPE = 'CSV_FILE_EXTRACTOR'

    def __init__(self, config_dict, transformer):
        super().__init__(config_dict, transformer)
        self.read_mode: Literal['single', 'all'] = config_dict.get('read_mode', 'single')

    def _read_csv(self, fs: AbstractFileSystem, source: str | list[str]) -> Table:
        self.logger.info(f"extract files using 'package' = {self.use_package}")

        package_mgr: dict = {
            'arrow': self.__use_arrow,
            'pandas': self.__use_pandas,
            'polars': self.__use_polars,
            'duckdb': self.__use_duckdb
        }

        if self.storage_backend != 'fs' or self.use_package == 'arrow_ds':
            return self.__use_arrow_dataset(fs, source)
        else:
            fs_reader = package_mgr[self.use_package]
            return fs_reader(source)

    def __use_arrow_dataset(self, fs: AbstractFileSystem, source: str | list[str]):
        return ds.dataset(
            source=source,
            format='csv',
            exclude_invalid_files=True,
            filesystem=fs
        ).to_table()

    def __use_arrow(self, source: str | list[str]) -> Table:
        if isinstance(source, list):
            dfs = [pc.read_csv(file) for file in source]
            df = pa.concat_tables(dfs)
        else:
            df = pc.read_csv()

        return df

    def __use_pandas(self, source: str | list[str]) -> Table:
        if isinstance(source, list):
            dfs = [pd.read_csv(file) for file in source]
            df = pd.concat(dfs, ignore_index=True)
        else:
            df = pd.read_csv(source)

        return pa.Table.from_pandas(df)

    def __use_polars(self, source: str | list[str]) -> Table:
        if isinstance(source, list):
            dfs = [pl.read_csv(file) for file in source]
            df = pl.concat(dfs)
        else:
            df = pl.read_csv(source)

        return df.to_arrow()

    def __use_duckdb(self, source: str | list[str]) -> Table:
        df = duckdb.sql(f'select * from read_csv({source})')
        return df.arrow()

    def do_extract(self):
        if self.storage_backend == 'fs':
            self.logger.info(f"extract files using 'storage_backend' = [{self.storage_backend}]")

            fs = LocalFileSystem(auto_mkdir=True)
            files = self._list_files(fs, file_ext='csv')

            if not bool(files):
                return None

            if self.read_mode == 'all' or len(files) == 1:
                dataset = self._read_csv(fs, files)
                self._update_hf_file(fs, files)
                return dataset
            else:
                for file in files:
                    dataset = self._read_csv(fs, file)
                    self.call_transformer(dataset)

                self._update_hf_file(fs, files)
                self.is_transformed = True
                return None
        else:
            raise NotImplementedError()
