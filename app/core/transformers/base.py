import logging
from time import perf_counter_ns
from abc import (
    ABC,
    abstractmethod
)

from ..storages import BaseStorage
from ...utils import format_perf_ns_to_time


class BaseTransformer(ABC):
    TYPE = 'BASE_TRANSFORMER'

    def __init__(self, config_dict: dict, storage: BaseStorage | None) -> None:
        super().__init__()
        self.config_dict = config_dict
        self.storage = storage
        self.name = self.config_dict['name']
        self.logger = logging.getLogger(self.name)

    def _transform(self, data):
        start_ns = perf_counter_ns()

        data = self.do_transform(data=data)

        end_ns = perf_counter_ns()
        elapsed_ns = end_ns - start_ns
        formatted_time = format_perf_ns_to_time(elapsed_ns)

        self.logger.info(f'transformer [{self.name}] completed in {formatted_time}')

        if self.storage is None:
            return data

        return self.storage._store(data=data)

    @abstractmethod
    def do_transform(self, data):
        raise NotImplementedError()


__all__ = ['BaseTransformer']
