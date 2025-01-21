from .base import BaseStorage
from .files import CsvStorage


class StorageManager(object):
    STORAGES: dict = {
        BaseStorage.TYPE: BaseStorage,
        CsvStorage.TYPE: CsvStorage
    }

    @classmethod
    def create_storage(cls, config: dict) -> BaseStorage:
        storage_type = config['type']
        storage_factory = cls.STORAGES.get(storage_type, None)

        assert storage_factory is not None, f'unknown storage type: [{storage_type}]'

        storage: BaseStorage = storage_factory(
            config_dict=config
        )

        storage.logger.info(f'created storage [{storage_type}]')
        return storage
