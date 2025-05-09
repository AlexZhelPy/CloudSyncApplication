from pathlib import Path
from typing import Dict, Tuple
from utils.logger import setup_logger

logger = setup_logger(__name__)

class LocalScanner:
    """Класс для сканирования локальной файловой системы.
    
    Обеспечивает обнаружение файлов в указанной директории и сбор их метаданных.
    """

    def __init__(self, local_path: Path):
        """Инициализация сканера локальных файлов.
        
        Args:
            local_path: Путь к корневой директории для сканирования.
                        Все файлы будут сканироваться относительно этого пути.
        """
        self.local_path = local_path

    def scan_local_files(self) -> Dict[str, Tuple[float, int]]:
        """Сканирует все файлы в указанной директории и возвращает их метаданные.
        
        Проходит рекурсивно по всем поддиректориям, собирая информацию о файлах:
        - Относительный путь (в формате POSIX с '/' как разделителем)
        - Время последней модификации (timestamp)
        - Размер файла в байтах

        Returns:
            Словарь, где:
            - Ключ: относительный путь к файлу (str)
            - Значение: кортеж (время_модификации, размер_файла)

        Пример возвращаемого значения:
            {
                "file.txt": (1680000000.0, 1024),
                "folder/file.jpg": (1680000001.0, 2048)
            }

        Примечания:
            - Пропускает файлы, которые не удалось прочитать
            - Логирует предупреждения для проблемных файлов
            - Возвращает только файлы (директории игнорируются)
        """
        files = {}
        for path in self.local_path.rglob('*'):
            if path.is_file():
                try:
                    # Конвертируем путь в POSIX-формат с '/' как разделителем
                    rel_path = str(path.relative_to(self.local_path)).replace('\\', '/')
                    stat = path.stat()
                    files[rel_path] = (stat.st_mtime, stat.st_size)
                    logger.debug(f"Найден локальный файл: {rel_path}")
                except OSError as e:
                    logger.warning(f"Не удалось прочитать файл {path}: {e}")
        return files
    