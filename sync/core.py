from pathlib import Path
from typing import Dict, Tuple, Any

from .local_scanner import LocalScanner
from .cloud_ops import CloudOperations
from .change_detector import ChangeDetector
from utils.logger import setup_logger
from utils.exceptions import SyncError

logger = setup_logger(__name__)

class FileSynchronizer:
    """Основной класс для синхронизации файлов между локальной файловой системой и облачным хранилищем.
    
    Обеспечивает полный цикл синхронизации:
    - Первоначальную настройку и синхронизацию
    - Отслеживание изменений
    - Синхронизацию измененных файлов
    - Обработку ошибок и валидацию результатов
    """

    def __init__(self, local_path: Path, cloud_client: Any):
        """Инициализация синхронизатора.
        
        Args:
            local_path: Путь к локальной директории для синхронизации
            cloud_client: Клиент для работы с облачным хранилищем
        """
        self.local_path = local_path
        self.cloud_ops = CloudOperations(cloud_client, local_path)
        self.local_scanner = LocalScanner(local_path)
        self.detector = ChangeDetector()
        self._last_local_state: Dict[str, Tuple[float, int]] = {}
        self._last_cloud_state: Dict[str, Dict[str, Any]] = {}
        logger.info(f"Инициализирован синхронизатор для папки {local_path}")

    def initial_sync(self) -> None:
        """Выполняет первоначальную синхронизацию между локальной и облачной файловыми системами.
        
        Процесс состоит из следующих этапов:
        1. Сканирование локальных файлов
        2. Сканирование облачных файлов
        3. Очистка облачного хранилища (если там есть файлы)
        4. Создание структуры папок в облаке
        5. Загрузка всех файлов в облако
        6. Валидация результатов синхронизации
        
        Raises:
            SyncError: Если произошла ошибка на любом из этапов синхронизации
        """
        logger.info("=== Начало первоначальной синхронизации ===")
        try:
            logger.info("Этап 1: Сканирование локальных файлов")
            local_files = self.local_scanner.scan_local_files()
            if not local_files:
                logger.warning("Локальная папка пуста - нечего синхронизировать")
                return

            logger.info("Этап 2: Сканирование облачных файлов")
            cloud_files = self.cloud_ops.scan_cloud_files_with_retry(max_retries=5)

            if cloud_files:
                logger.info("Этап 3: Очистка облачного хранилища")
                self.cloud_ops.clean_cloud_storage(cloud_files)

            logger.info("Этап 4: Создание структуры папок")
            self.cloud_ops.create_folder_structure(list(local_files.keys()))

            logger.info("Этап 5: Загрузка файлов в облако")
            self.cloud_ops.upload_all_files(local_files)

            logger.info("Этап 6: Валидация синхронизации")
            self.cloud_ops.validate_sync(local_files)

            self._last_local_state = local_files
            self._last_cloud_state = self.cloud_ops.scan_cloud_files_with_retry()
            logger.info("=== Первоначальная синхронизация успешно завершена ===")
        except Exception as e:
            logger.error(f"Ошибка при первоначальной синхронизации: {e}")
            raise SyncError(f"Ошибка при первоначальной синхронизации: {e}")

    def sync(self) -> None:
        """Выполняет инкрементальную синхронизацию, обрабатывая только изменения.
        
        Процесс работы:
        1. Сканирование текущего состояния локальных и облачных файлов
        2. Сравнение с предыдущим состоянием для обнаружения изменений
        3. Обработка изменений (если они обнаружены)
        4. Обновление информации о последнем состоянии
        
        Raises:
            SyncError: Если произошла ошибка в процессе синхронизации
        """
        logger.info("Проверка изменений...")
        try:
            current_local = self.local_scanner.scan_local_files()
            current_cloud = self.cloud_ops.scan_cloud_files_with_retry()
            local_changes = self.detector.check_local_changes(current_local, self._last_local_state)
            cloud_changes = self.detector.check_cloud_changes(current_cloud, self._last_cloud_state)
            if not local_changes and not cloud_changes:
                logger.info("Изменений не обнаружено")
                return
            logger.info("Обнаружены изменения. Начинаю синхронизацию...")
            if local_changes:
                logger.info("Обнаружены локальные изменения")
                self.detector.process_local_changes(
                    current_local, current_cloud, self._last_local_state,
                    self.local_path, self.cloud_ops, self.cloud_ops.cloud_client
                )
            self._last_local_state = current_local
            self._last_cloud_state = current_cloud
            logger.info("Синхронизация завершена")
        except Exception as e:
            logger.error(f"Ошибка синхронизации: {e}")
            raise SyncError(f"Ошибка синхронизации: {e}")
        