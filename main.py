import time
from pathlib import Path

from config.settings import load_config
from cloud_storage.yandex_disk import YandexDiskClient
from sync.core import FileSynchronizer
from utils.logger import setup_logger

logger = setup_logger(__name__)


def main():
    try:
        # Загрузка конфигурации
        config = load_config()

        # Инициализация клиента облачного хранилища
        cloud_client = YandexDiskClient(config['token'], config['cloud_folder'])

        # Инициализация синхронизатора
        synchronizer = FileSynchronizer(Path(config['local_path']), cloud_client)

        # Первоначальная синхронизация
        synchronizer.initial_sync()

        # Основной цикл синхронизации
        sync_interval = config.get('sync_interval', 60)  # Интервал по умолчанию - 60 секунд
        logger.info(f"Переход в режим периодической синхронизации (интервал: {sync_interval} сек)")

        while True:
            try:
                logger.info("--- Проверка изменений ---")
                # Проверяем изменения и синхронизируем при необходимости
                synchronizer.sync()
                logger.info(f"Следующая проверка через {sync_interval} секунд...")
            except Exception as e:
                logger.error(f"Ошибка при синхронизации: {e}")
                # В случае ошибки продолжаем работу после паузы
                logger.info(f"Повторная попытка через {sync_interval} секунд...")

            # Ожидание перед следующей проверкой
            time.sleep(sync_interval)

    except KeyboardInterrupt:
        logger.info("Синхронизация остановлена пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
    