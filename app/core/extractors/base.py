import logging
from time import perf_counter_ns
from abc import (
    ABC,
    abstractmethod
)

from ..transformers import BaseTransformer
from ...utils import format_perf_ns_to_time


class BaseExtractor(ABC):
    TYPE = 'BASE_EXTRACTOR'

    def __init__(self, config_dict: dict, transformer: BaseTransformer) -> None:
        super().__init__()
        self.is_transformed = False
        self.config_dict = config_dict
        self.transformer = transformer
        self.name = self.config_dict['name']
        self.logger = logging.getLogger(self.name)

    def _extract(self):
        start_ns = perf_counter_ns()

        data = self.do_extract()

        end_ns = perf_counter_ns()
        elapsed_ns = end_ns - start_ns
        formatted_time = format_perf_ns_to_time(elapsed_ns)

        self.logger.info(f'extractor [{self.name}] completed in {formatted_time}')

        if data is not None:
            self.call_transformer(data)
            self.is_transformed = True

        if not self.is_transformed:
            self.logger.warning(f'process terminated: no data found to proceed')

    def call_transformer(self, data):
        self.transformer._transform(data)

    @abstractmethod
    def do_extract(self):
        raise NotImplementedError()


__all__ = ['BaseExtractor']
