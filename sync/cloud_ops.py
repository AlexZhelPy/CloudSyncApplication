import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from utils.logger import setup_logger
from utils.exceptions import SyncError

logger = setup_logger(__name__)

class CloudOperations:
    """Класс для выполнения операций с облачным хранилищем.
    
    Обеспечивает сканирование, загрузку, удаление файлов и другие операции
    синхронизации между локальной файловой системой и облачным хранилищем.
    """
    
    def __init__(self, cloud_client: Any, local_path: Path):
        """Инициализация операций с облачным хранилищем.
        
        Args:
            cloud_client: Клиент для работы с облачным хранилищем
            local_path: Локальный путь к синхронизируемой директории
        """
        self.cloud_client = cloud_client
        self.local_path = local_path
        self.cloud_folder = getattr(cloud_client, 'cloud_folder', '')

    def scan_cloud_files_with_retry(self, max_retries: int = 3) -> Optional[Dict[str, Dict[str, Any]]]:
        """Сканирует файлы в облачном хранилище с возможностью повторных попыток.
        
        Args:
            max_retries: Максимальное количество попыток сканирования
            
        Returns:
            Словарь с информацией о файлах в облаке (путь -> метаданные)
            
        Raises:
            Exception: Если не удалось сканировать после всех попыток
        """
        for attempt in range(max_retries):
            try:
                cloud_files = {}
                items = self.cloud_client.get_recursive_info()
                for item in items:
                    if item['type'] == 'file':
                        path = item['path'].replace(f"disk:/{self.cloud_folder}/", "")
                        cloud_files[path] = item
                        logger.debug(f"Найден облачный файл: {path}")
                return cloud_files
            except Exception:
                if attempt == max_retries - 1:
                    logger.error(f"Не удалось сканировать облако после {max_retries} попыток")
                    raise
                wait_time = 5 * (attempt + 1)
                logger.warning(f"Ошибка сканирования облака (попытка {attempt + 1}), ждем {wait_time} сек...")
                time.sleep(wait_time)

    def clean_cloud_storage(self, cloud_files: Dict[str, Dict[str, Any]]) -> None:
        """Полностью очищает облачное хранилище от файлов.
        
        Args:
            cloud_files: Словарь с информацией о файлах для удаления
            
        Raises:
            SyncError: Если произошла ошибка при удалении файлов
        """
        total_files = len(cloud_files)
        logger.info(f"Начинаю удаление {total_files} файлов из облака")
        for i, (rel_path, item) in enumerate(cloud_files.items(), 1):
            try:
                self.cloud_client.delete(item['path'])
                logger.info(f"[{i}/{total_files}] Удален файл: {rel_path}")
            except Exception as e:
                logger.error(f"Ошибка при удалении файла {rel_path}: {e}")
                raise SyncError(f"Не удалось очистить облако: ошибка при удалении {rel_path}")
        time.sleep(10)
        logger.info("Очистка облака завершена")

    def create_folder_structure(self, file_paths: List[str]) -> None:
        """Создает структуру папок в облачном хранилище на основе списка путей.
        
        Args:
            file_paths: Список относительных путей к файлам
            
        Raises:
            SyncError: Если произошла ошибка при создании папок
        """
        folders = set()
        for path in file_paths:
            if '/' in path:
                parts = path.split('/')[:-1]
                current_path = ""
                for part in parts:
                    current_path = f"{current_path}/{part}" if current_path else part
                    folders.add(current_path)
        sorted_folders = sorted(folders, key=lambda x: x.count('/'))
        for folder in sorted_folders:
            try:
                self.cloud_client.create_folder(folder)
                logger.debug(f"Создана папка: {folder}")
            except Exception as e:
                logger.error(f"Ошибка при создании папки {folder}: {e}")
                raise SyncError(f"Не удалось создать папку {folder}")

    def upload_all_files(self, local_files: Dict[str, Tuple[float, int]]) -> None:
        """Загружает все файлы из локальной директории в облачное хранилище.
        
        Args:
            local_files: Словарь с информацией о локальных файлах (путь -> (mtime, size))
        """
        total_files = len(local_files)
        logger.info(f"Начинаю загрузку {total_files} файлов в облако")
        for i, rel_path in enumerate(local_files, 1):
            self.upload_file(rel_path, max_retries=3, log_progress=(i, total_files))
        time.sleep(15)

    def upload_file(self, rel_path: str, is_update: bool = False, max_retries: int = 3,
                   log_progress: Optional[Tuple[int, int]] = None) -> None:
        """Загружает или обновляет один файл в облачном хранилище.
        
        Args:
            rel_path: Относительный путь к файлу
            is_update: Флаг, указывающий на обновление существующего файла
            max_retries: Максимальное количество попыток загрузки
            log_progress: Кортеж (текущий_номер, всего) для логирования прогресса
            
        Raises:
            SyncError: Если файл не существует или произошла ошибка загрузки
        """
        file_path = self.local_path / rel_path
        if not file_path.exists():
            logger.warning(f"Локальный файл {rel_path} не существует, пропускаем загрузку")
            return

        try:
            stat = file_path.stat()
            current_mtime, current_size = stat.st_mtime, stat.st_size
        except OSError as e:
            logger.error(f"Не удалось получить метаданные файла {rel_path}: {e}")
            raise SyncError(f"Ошибка чтения файла {rel_path}")

        for attempt in range(max_retries):
            try:
                if is_update:
                    log_msg = f"Обновление файла {rel_path}"
                    if log_progress:
                        log_msg = f"[{log_progress[0]}/{log_progress[1]}] {log_msg}"
                    logger.info(log_msg)
                    self.cloud_client.reload(file_path, rel_path)
                else:
                    log_msg = f"Загрузка файла {rel_path}"
                    if log_progress:
                        log_msg = f"[{log_progress[0]}/{log_progress[1]}] {log_msg}"
                    logger.info(log_msg)
                    self.cloud_client.load(file_path, rel_path)
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Не удалось загрузить файл {rel_path} после {max_retries} попыток")
                    raise SyncError(f"Ошибка загрузки файла {rel_path}: {e}")
                wait_time = 2 * (attempt + 1)
                logger.warning(f"Ошибка загрузки {rel_path}, попытка {attempt + 1}. Ждем {wait_time} сек...")
                time.sleep(wait_time)

    def validate_sync(self, local_files: Dict[str, Tuple[float, int]]) -> None:
        """Проверяет результаты синхронизации, сравнивая локальные и облачные файлы.
        
        Args:
            local_files: Словарь с информацией о локальных файлах
            
        Raises:
            SyncError: Если обнаружены расхождения между локальными и облачными файлами
        """
        logger.info("Проверка результатов синхронизации...")
        cloud_files = self.scan_cloud_files_with_retry(max_retries=5)
        logger.info(f"Локальные файлы: {set(local_files.keys())}")
        logger.info(f"Облачные файлы: {set(cloud_files.keys())}")
        missing_files = set(local_files.keys()) - set(cloud_files.keys())
        if missing_files:
            for file in missing_files:
                logger.error(f"Файл отсутствует в облаке: {file}")
            raise SyncError(f"Не удалось загрузить {len(missing_files)} файлов в облако")
        logger.info("Все файлы успешно синхронизированы")
        