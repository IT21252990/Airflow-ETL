import os
import sys
import yaml
import argparse
import logging

from fsspec.implementations.local import LocalFileSystem
from logging import Logger
from time import perf_counter_ns
from confuse import Configuration

from .utils import format_perf_ns_to_time

from .core.storages import StorageManager
from .core.transformers import TransformerManager
from .core.extractors import ExtractorManager


def run_etl(config: Configuration, logger: Logger):
    logger.info(f'initializing etl pipeline')

    app_name: str = config['app_name'].get(str)

    storage_config: dict = config['storage'].get(dict)
    storage = StorageManager.create_storage(
        config=storage_config
    )

    transformer_config: dict = config['transformer'].get(dict)
    transformer = TransformerManager.create_transformer(
        config=transformer_config,
        storage=storage
    )

    extractor_config: dict = config['extract'].get(dict)
    extractor = ExtractorManager.create_extractor(
        config=extractor_config,
        transformer=transformer
    )

    extractor._extract()


def main(args):
    start_ns = perf_counter_ns()

    file_name = os.path.basename(__file__).split('.')[0]

    logger = logging.getLogger(file_name)
    logger.info('initializing application')

    config_loc = args.config_loc
    config_path = args.config_path
    config_name = args.config_name

    current_path = os.path.dirname(os.path.abspath(__file__))
    local_config = os.path.join(current_path, config_name)
    config_root = config_name.split('.')[0]

    if config_loc == 'fs':
        fs = LocalFileSystem()
        config_fn = os.path.join(os.path.abspath(config_path), config_name)
    elif config_loc in ['s3', 'gcs', 'abs']:
        raise NotImplementedError()

    logger.info(f"use 'config_loc' = {config_loc} to load config")

    fs.copy(config_fn, local_config)
    logger.info(f'copy config file from [{config_fn}] to [{local_config}]')

    app_config = Configuration(config_root)
    app_config.set_file(local_config)

    logger.info(f'load config file from [{local_config}]')

    app_name: str = app_config['app_name'].get(str)
    log_level: str = app_config['meta']['logging_level'].get(str)

    logging.getLogger().setLevel(log_level.upper())
    logger = logging.getLogger(app_name)

    run_etl(app_config, logger)

    end_ns = perf_counter_ns()
    elapsed_ns = end_ns - start_ns
    formatted_time = format_perf_ns_to_time(elapsed_ns)

    os.remove(local_config)
    logger.info(f'config file [{local_config}] deleted after processing')

    logger.info(f'completed run in {formatted_time}')
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-10s | %(name)-20s | %(message)s'
    )

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config-loc',
        help='configuration file location',
        required=False,
        default='fs',
        choices=['fs', 's3', 'gcs', 'abs']
    )
    parser.add_argument(
        '--config-path',
        help='configuration file path',
        required=False,
        default=os.getcwd()
    )
    parser.add_argument(
        '--config-name',
        help='configuration file name',
        required=True
    )

    args = parser.parse_args()
    res = -1

    file_name = os.path.basename(__file__).split('.')[0]
    logger = logging.getLogger(file_name)

    try:
        res = main(args)
    except Exception as e:
        logger.error(e)
        logger.exception(f'exit code = {res}')
    finally:
        logger.info(f'exit code = {res}')
        sys.exit(res)
