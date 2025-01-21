from .base import BaseExtractor
from .files import (
    CsvFileExtractor
)

from ..transformers import BaseTransformer


class ExtractorManager(object):
    EXTRACTORS: dict = {
        BaseExtractor.TYPE: BaseExtractor,
        CsvFileExtractor.TYPE: CsvFileExtractor
    }

    @classmethod
    def create_extractor(cls, config: dict, transformer: BaseTransformer) -> BaseExtractor:
        extractor_type = config['type']
        extractor_factory = cls.EXTRACTORS.get(extractor_type, None)

        assert extractor_factory is not None, f'unknown extractor type: [{extractor_type}]'

        extractor: BaseExtractor = extractor_factory(
            config_dict=config,
            transformer=transformer
        )

        extractor.logger.info(f'created extractor [{extractor_type}]')
        return extractor
