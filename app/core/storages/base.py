import logging
from time import perf_counter_ns
from abc import (
    ABC,
    abstractmethod
)

from ...utils import format_perf_ns_to_time


class BaseStorage(ABC):
    TYPE = 'BASE_STORAGE'

    def __init__(self, config_dict: dict) -> None:
        super().__init__()
        self.config_dict = config_dict
        self.name = self.config_dict['name']
        self.logger = logging.getLogger(self.name)

    def _store(self, data):
        start_ns = perf_counter_ns()

        response = self.do_store(data)

        end_ns = perf_counter_ns()
        elapsed_ns = end_ns - start_ns
        formatted_time = format_perf_ns_to_time(elapsed_ns)

        self.logger.info(f'storage [{self.name}] completed in {formatted_time}')

    @abstractmethod
    def do_store(self, data):
        raise NotImplementedError()


__all__ = ['BaseStorage']
