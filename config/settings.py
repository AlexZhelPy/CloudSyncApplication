import configparser
from pathlib import Path
from typing import Dict, Any

from utils.exceptions import ConfigError
from utils.logger import setup_logger

logger = setup_logger(__name__)

def load_config(config_path: str = 'config.ini') -> Dict[str, Any]:
    """
    Загружает и валидирует конфигурацию из файла.

    Args:
        config_path: Путь к конфигурационному файлу.

    Returns:
        Словарь с настройками.

    Raises:
        ConfigError: Если конфигурация невалидна.
    """
    config = configparser.ConfigParser()
    if not config.read(config_path):
        raise ConfigError(f"Конфигурационный файл {config_path} не найден")

    try:
        settings = {
            'local_path': Path(config['DEFAULT']['LocalPath']),
            'cloud_folder': config['DEFAULT']['CloudFolder'],
            'token': config['DEFAULT']['Token'],
            'sync_interval': int(config['DEFAULT'].get('SyncInterval', 60)),
            'log_file': config['DEFAULT'].get('LogFile', 'sync.log')
        }
    except KeyError as e:
        raise ConfigError(f"Отсутствует обязательный параметр: {e}")

    # Валидация пути
    if not settings['local_path'].exists():
        raise ConfigError(f"Локальная папка {settings['local_path']} не существует")

    logger.info("Конфигурация успешно загружена")
    return settings
