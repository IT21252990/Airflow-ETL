import duckdb
import polars as pl

from typing import Literal
from pyarrow import Table

from .base import BaseTransformer


class NoOpTransformer(BaseTransformer):
    TYPE = 'NO_OP_TRANSFORMER'

    def __init__(self, config_dict, storage):
        super().__init__(config_dict, storage)

    def do_transform(self, data):
        self.logger.info(f'initiating data transformation task')
        return data


class QueryTransformer(BaseTransformer):
    TYPE = 'QUERY_TRANSFORMER'

    def __init__(self, config_dict, storage):
        super().__init__(config_dict, storage)
        self.use_package: Literal['polars', 'duckdb'] = self.config_dict['use_package']
        self.query: str = self.config_dict['query']
        self.table_name: str = self.config_dict['table_name']

    def __use_duckdb(self, data: Table) -> Table:
        # always replace table_name with the argument name or variable name for arrow.Table
        # (self, data: Table) ---- [use 'data']

        query = self.query.replace(self.table_name, 'data')

        return duckdb.sql(query).arrow()

    def __use_polars(self, data: Table) -> Table:
        # always replace table_name with the variable name used in pl.SQLContext to register frames
        # pl.SQLContext(frames={'data': pl.from_arrow(data)}) ---- [use 'data']

        query = self.query.replace(self.table_name, 'data')

        ctx = pl.SQLContext(frames={'data': pl.from_arrow(data)})
        return ctx.execute(query, eager=True).to_arrow()

    def do_transform(self, data: Table) -> Table:
        self.logger.info(f"use 'package' = [{self.use_package}] to transform data")

        package_mgr: dict = {
            'polars': self.__use_polars,
            'duckdb': self.__use_duckdb
        }

        fn = package_mgr[self.use_package]
        return fn(data)
