import time
from pathlib import Path
from typing import Dict, Tuple, Any, List
from utils.logger import setup_logger

logger = setup_logger(__name__)

class ChangeDetector:
    """Класс для обнаружения и обработки изменений между локальными и облачными файлами.
    
    Отслеживает изменения в файловой системе и синхронизирует их с облачным хранилищем,
    обрабатывая операции добавления, удаления, изменения и переименования файлов и папок.
    """
    
    def __init__(self):
        """Инициализация детектора изменений.
        
        Создает пустые словари для хранения последних известных состояний локальных
        и облачных файлов.
        """
        self._last_local_state: Dict[str, Tuple[float, int]] = {}
        self._last_cloud_state: Dict[str, Dict[str, Any]] = {}
        self.cloud_client = None  # Будет установлен при вызове process_*_changes

    def check_local_changes(self, current_local: Dict[str, Tuple[float, int]], last_local: Dict[str, Tuple[float, int]]) -> bool:
        """Проверяет наличие изменений в локальных файлах по сравнению с последним известным состоянием.
        
        Args:
            current_local: Текущее состояние локальных файлов (путь -> (время модификации, размер))
            last_local: Последнее известное состояние локальных файлов
            
        Returns:
            bool: True если обнаружены изменения, иначе False
        """
        if not last_local:
            logger.debug("Нет данных о предыдущем состоянии локальных файлов")
            return True
        added_files = set(current_local.keys()) - set(last_local.keys())
        removed_files = set(last_local.keys()) - set(current_local.keys())
        if added_files:
            logger.info(f"Обнаружены новые файлы: {added_files}")
            return True
        if removed_files:
            logger.info(f"Обнаружены удаленные файлы: {removed_files}")
            return True
        for path, (mtime, size) in current_local.items():
            if path in last_local:
                last_mtime, last_size = last_local[path]
                if mtime > last_mtime or size != last_size:
                    logger.info(f"Обнаружены изменения в файле: {path}")
                    return True
        return False

    def check_cloud_changes(self, current_cloud: Dict[str, Dict[str, Any]], last_cloud: Dict[str, Dict[str, Any]]) -> bool:
        """Проверяет наличие изменений в облачном хранилище по сравнению с последним известным состоянием.
        
        Args:
            current_cloud: Текущее состояние облачных файлов (путь -> метаданные)
            last_cloud: Последнее известное состояние облачных файлов
            
        Returns:
            bool: True если обнаружены изменения, иначе False
        """
        if not last_cloud:
            logger.debug("Нет данных о предыдущем состоянии облачных файлов")
            return True
        added = set(current_cloud.keys()) - set(last_cloud.keys())
        removed = set(last_cloud.keys()) - set(current_cloud.keys())
        if added or removed:
            logger.info(f"Изменения в облаке: добавлены {added}, удалены {removed}")
            return True
        for path, item in current_cloud.items():
            if path in last_cloud and item.get('modified') != last_cloud[path].get('modified'):
                logger.info(f"Изменен файл в облаке: {path}")
                return True
        return False

    def process_local_changes(self, current_local, current_cloud, last_local, local_path, cloud_ops, cloud_client):
        """Обрабатывает изменения в локальных файлах, синхронизируя их с облачным хранилищем.
        
        Выполняет операции в следующем порядке:
        1. Переименование папок
        2. Переименование файлов
        3. Загрузка новых и измененных файлов
        4. Удаление отсутствующих файлов
        
        Args:
            current_local: Текущее состояние локальных файлов
            current_cloud: Текущее состояние облачных файлов
            last_local: Последнее известное состояние локальных файлов
            local_path: Путь к локальной директории для синхронизации
            cloud_ops: Операции с облачным хранилищем
            cloud_client: Клиент для работы с облачным хранилищем
        """
        self.cloud_client = cloud_client
        current_local_set = set(current_local.keys())
        current_cloud_set = set(current_cloud.keys())
        last_local_set = set(last_local.keys()) if last_local else set()
        renamed_pairs = self._find_renamed_files(current_local, current_cloud, last_local, local_path)
        folder_renames = self._find_renamed_folders(current_local, current_cloud, last_local)
        
        processed_files = set()
        
        # 1. Переименование папок
        if folder_renames:
            # Сортируем по уровню вложенности (больше '/' = глубже)
            folder_renames.sort(key=lambda x: -x[0].count('/'))
            
            for old_folder, new_folder in folder_renames:
                try:
                    time.sleep(1)  # Небольшая задержка между операциями
                    self._process_folder_rename(old_folder, new_folder, current_cloud, cloud_client, local_path)
                except Exception as e:
                    logger.error(f"Ошибка переименования папки {old_folder} -> {new_folder}: {e}")
                    
        time.sleep(5)  
        
        # 2. Переименование файлов          
        if renamed_pairs:    
            for old_name, new_name in renamed_pairs:
                try:
                    time.sleep(1)
                    if self._try_rename_cloud_file(old_name, new_name, cloud_client):
                        logger.info(f"Файл переименован в облаке: {old_name} -> {new_name}")
                    else:
                        cloud_client.load(local_path / new_name, new_name)
                        if old_name in current_cloud:
                            cloud_client.delete(current_cloud[old_name]['path'])
                        logger.info(f"Файл перезагружен с новым именем: {old_name} -> {new_name}")
                except Exception as e:
                    logger.error(f"Ошибка обработки переименования {old_name} -> {new_name}: {e}")

            processed_files = {new_name for _, new_name in renamed_pairs}
            processed_files.update({old_name for old_name, _ in renamed_pairs})    

        # 3. Новые и изменённые файлы
        for rel_path in current_local_set - processed_files:
            file_path = local_path / rel_path
            if rel_path not in current_cloud_set:
                cloud_client.load(file_path, rel_path)
                logger.info(f"Загружен новый файл: {rel_path}")
            else:
                local_mtime, local_size = current_local[rel_path]
                cloud_item = current_cloud[rel_path]
                cloud_mtime = self._parse_cloud_time(cloud_item['modified'])
                if rel_path in last_local_set and (local_mtime > last_local[rel_path][0] or local_size != last_local[rel_path][1]):
                    cloud_client.load(file_path, rel_path)
                    logger.info(f"Обновлён файл: {rel_path}")
                elif local_mtime > cloud_mtime:
                    cloud_client.load(file_path, rel_path)
                    logger.info(f"Обновлён файл (по сравнению с облаком): {rel_path}")

        # 4. Удалённые файлы
        for rel_path in current_cloud_set - current_local_set - processed_files:
            cloud_client.delete(current_cloud[rel_path]['path'])
            logger.info(f"Удалён файл из облака: {rel_path}")

    def _find_renamed_folders(self, current_local, current_cloud, last_local) -> List[Tuple[str, str]]:
        """Находит переименованные папки путем сравнения структур до и после изменений.
        
        Args:
            current_local: Текущее состояние локальных файлов
            current_cloud: Текущее состояние облачных файлов
            last_local: Последнее известное состояние локальных файлов
            
        Returns:
            List[Tuple[str, str]]: Список кортежей (старое_имя_папки, новое_имя_папки)
        """
        renamed_folders = []

        # Получаем все папки (включая вложенные)
        last_folders = {
            '/'.join(f.split('/')[:i + 1])
            for f in last_local
            for i in range(len(f.split('/')) - 1)
        }

        current_folders = {
            '/'.join(f.split('/')[:i + 1])
            for f in current_local
            for i in range(len(f.split('/')) - 1)
        }

        disappeared_folders = last_folders - current_folders
        appeared_folders = current_folders - last_folders

        # Используем копию для итерации
        for old_folder in disappeared_folders:
            for new_folder in list(appeared_folders):  # Итерируем по копии
                if self._compare_folder_structures(old_folder, new_folder, current_local, current_cloud):
                    renamed_folders.append((old_folder, new_folder))
                    appeared_folders.remove(new_folder)  # Удаляем из оригинального множества
                    break

        logger.debug(f"Найдены переименованные папки: {renamed_folders}")
        return renamed_folders

    @staticmethod
    def _compare_folder_structures(old_folder: str, new_folder: str, current_local, current_cloud) -> bool:
        """Сравнивает структуры двух папок для определения возможного переименования.
        
        Args:
            old_folder: Путь к старой папке
            new_folder: Путь к новой папке
            current_local: Текущее состояние локальных файлов
            current_cloud: Текущее состояние облачных файлов
            
        Returns:
            bool: True если структуры папок идентичны (кроме имени), иначе False
        """
        old_files = {
            f[len(old_folder) + 1:]: d
            for f, d in current_cloud.items()
            if f.startswith(old_folder + '/')
        }
        new_files = {
            f[len(new_folder) + 1:]: (mtime, size)
            for f, (mtime, size) in current_local.items()
            if f.startswith(new_folder + '/')
        }

        # Быстрая проверка по количеству файлов
        if len(old_files) != len(new_files):
            return False

        # Проверяем совпадение файлов
        for rel_path, (local_mtime, local_size) in new_files.items():
            if rel_path not in old_files:
                return False
            if local_size != old_files[rel_path]['size']:
                return False

        return True

    def _process_folder_rename(self, old_folder: str, new_folder: str, current_cloud, cloud_client, local_path):
        """Выполняет операцию переименования папки в облачном хранилище.
        
        Args:
            old_folder: Старое имя папки
            new_folder: Новое имя папки
            current_cloud: Текущее состояние облачных файлов
            cloud_client: Клиент облачного хранилища
            local_path: Локальный путь к синхронизируемой директории
        """
        old_path = f"/{cloud_client.cloud_folder}/{old_folder}"
        new_path = f"/{cloud_client.cloud_folder}/{new_folder}"

        try:
            if hasattr(cloud_client, 'rename'):
                cloud_client.rename(old_path, new_path)
                logger.info(f"Папка переименована: {old_folder} -> {new_folder}")
            else:
                raise Exception("Клиент не поддерживает прямое переименование папок")
        except Exception as e:
            logger.error(f"Ошибка переименования папки {old_folder} -> {new_folder}: {e}")
            raise

    def _find_renamed_files(self, current_local, current_cloud, last_local, local_path) -> List[Tuple[str, str]]:
        """Находит переименованные файлы путем сравнения их идентификаторов.
        
        Args:
            current_local: Текущее состояние локальных файлов
            current_cloud: Текущее состояние облачных файлов
            last_local: Последнее известное состояние локальных файлов
            local_path: Локальный путь к синхронизируемой директории
            
        Returns:
            List[Tuple[str, str]]: Список кортежей (старое_имя, новое_имя)
        """
        renamed_pairs = []
        current_local_set = set(current_local.keys())
        current_cloud_set = set(current_cloud.keys())
        last_local_set = set(last_local.keys()) if last_local else set()
        disappeared_locally = last_local_set - current_local_set
        appeared_locally = current_local_set - last_local_set
        for old_name in disappeared_locally:
            if old_name not in current_cloud_set:
                continue
            old_file_hash = self._get_file_identifier(old_name, last_local)
            for new_name in list(appeared_locally):
                new_file_path = local_path / new_name
                if not new_file_path.exists():
                    continue
                new_file_hash = self._calculate_file_identifier(new_file_path)
                if old_file_hash == new_file_hash:
                    renamed_pairs.append((old_name, new_name))
                    appeared_locally.remove(new_name)
                    break
        return renamed_pairs

    @staticmethod
    def _get_file_identifier(rel_path: str, last_local) -> str:
        """Генерирует идентификатор файла на основе его метаданных.
        
        Args:
            rel_path: Относительный путь к файлу
            last_local: Последнее известное состояние локальных файлов
            
        Returns:
            str: Идентификатор файла в формате "размер-время_модификации"
        """
        if rel_path in last_local:
            mtime, size = last_local[rel_path]
            return f"{size}-{mtime}"
        return ""

    @staticmethod
    def _calculate_file_identifier(file_path: Path) -> str:
        """Вычисляет идентификатор для локального файла.
        
        Args:
            file_path: Полный путь к файлу
            
        Returns:
            str: Идентификатор файла в формате "размер-время_модификации"
        """
        try:
            stat = file_path.stat()
            return f"{stat.st_size}-{stat.st_mtime}"
        except OSError:
            return ""

    def _try_rename_cloud_file(self, old_name: str, new_name: str, cloud_client) -> bool:
        """Пытается переименовать файл в облачном хранилище.
        
        Args:
            old_name: Старое имя файла
            new_name: Новое имя файла
            cloud_client: Клиент облачного хранилища
            
        Returns:
            bool: True если переименование успешно, иначе False
        """
        try:
            if hasattr(cloud_client, 'rename'):
                old_path = f"/{cloud_client.cloud_folder}/{old_name}"
                new_path = f"/{cloud_client.cloud_folder}/{new_name}"
                cloud_client.rename(old_path, new_path)
                return True
        except Exception as e:
            logger.debug(f"Ошибка при переименовании файла {old_name} -> {new_name}: {e}")
        return False

    @staticmethod
    def _parse_cloud_time(timestr: str) -> float:
        """Преобразует строку времени из облачного хранилища в timestamp.
        
        Args:
            timestr: Строка с временем в формате ISO (например, '2023-04-27T19:41:25+00:00')
            
        Returns:
            float: Временная метка в секундах с начала эпохи
        """
        try:
            from datetime import datetime
            dt = datetime.strptime(timestr[:19], "%Y-%m-%dT%H:%M:%S")
            return dt.timestamp()
        except Exception:
            return 0.0
        