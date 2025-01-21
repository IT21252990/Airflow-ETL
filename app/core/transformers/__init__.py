from .base import BaseTransformer
from .common import (
    NoOpTransformer,
    QueryTransformer
)

from ..storages import BaseStorage


class ChainTransformer(BaseTransformer):
    TYPE = 'CHAIN_TRANSFORMER'

    def __init__(self, config_dict, storage):
        super().__init__(config_dict, storage)
        self.configs = config_dict['transforms']

        self.__transformers = [TransformerManager.create_transformer(cfg, None) for cfg in self.configs]

    def do_transform(self, data):
        for transformer in self.__transformers:
            data = transformer._transform(data=data)

        return data


class TransformerManager(object):
    TRANSFORMERS: dict = {
        BaseTransformer.TYPE: BaseTransformer,
        ChainTransformer.TYPE: ChainTransformer,
        NoOpTransformer.TYPE: NoOpTransformer,
        QueryTransformer.TYPE: QueryTransformer
    }

    @classmethod
    def create_transformer(cls, config: dict, storage: BaseStorage | None) -> BaseTransformer:
        transformer_type = config['type']
        transformer_factory = cls.TRANSFORMERS.get(transformer_type, None)

        assert transformer_factory is not None, f'unknown transformer type: [{transformer_type}]'

        transformer: BaseTransformer = transformer_factory(
            config_dict=config,
            storage=storage
        )

        transformer.logger.info(f'created transformer [{transformer_type}]')
        return transformer
