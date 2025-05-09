import requests
from pathlib import Path
from typing import Dict, List, Any

from utils.exceptions import CloudStorageError
from utils.logger import setup_logger

logger = setup_logger(__name__)


class YandexDiskClient:
    """Клиент для работы с Yandex Disk API."""

    BASE_URL = 'https://cloud-api.yandex.net/v1/disk'

    def __init__(self, token: str, cloud_folder: str):
        """
        Инициализация клиента.

        Args:
            token: OAuth-токен для доступа к Yandex Disk.
            cloud_folder: Папка в облачном хранилище для синхронизации.
        """
        self.token = token
        self.cloud_folder = cloud_folder
        self.headers = {'Authorization': f'OAuth {token}'}

        # Проверяем доступность хранилища при инициализации
        self._check_connection()

    def _check_connection(self) -> None:
        """Проверяет доступность облачного хранилища с новыми правами."""
        try:
            # Проверяем доступ к API
            response = requests.get(
                f'{self.BASE_URL}/',
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()

            # Проверяем наличие необходимых возможностей
            test_operations = [
                ('GET', f'{self.BASE_URL}/resources', {'path': '/'}),  # Проверка чтения
                ('PUT', f'{self.BASE_URL}/resources', {'path': f'/{self.cloud_folder}'})  # Проверка записи
            ]

            for method, url, params in test_operations:
                test_response = requests.request(
                    method,
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=10
                )

                if test_response.status_code == 403:
                    required_scope = 'disk.read' if method == 'GET' else 'disk.write'
                    raise CloudStorageError(
                        f"Недостаточно прав. Требуется право: cloud_api:disk.{required_scope}"
                    )

            # Проверяем существование папки (или создаём)
            folder_path = f'/{self.cloud_folder}'
            response = requests.get(
                f'{self.BASE_URL}/resources',
                headers=self.headers,
                params={'path': folder_path},
                timeout=10
            )

            if response.status_code == 404:
                response = requests.put(
                    f'{self.BASE_URL}/resources',
                    headers=self.headers,
                    params={'path': folder_path},
                    timeout=10
                )
                response.raise_for_status()
                logger.info(f"Создана новая папка в облаке: {self.cloud_folder}")

        except requests.HTTPError as e:
            if e.response.status_code == 401:
                raise CloudStorageError("Неверный токен. Получите новый токен с правами: "
                                        "cloud_api:disk.read, cloud_api:disk.write, cloud_api:disk.info")
            raise CloudStorageError(f"Ошибка HTTP при подключении: {e}")
        except Exception as e:
            raise CloudStorageError(f"Неожиданная ошибка: {e}")

    def load(self, file_path: Path, rel_path: str) -> None:
        """
        Загружает файл в облачное хранилище с учетом относительного пути.

        Args:
            file_path: Полный путь к локальному файлу.
            rel_path: Относительный путь для сохранения в облаке.
        """
        try:
            # Получаем URL для загрузки
            upload_url = self._get_upload_url(rel_path)

            # Загружаем файл
            with open(file_path, 'rb') as f:
                response = requests.put(upload_url, files={'file': f})
                response.raise_for_status()

            logger.info(f"Файл {rel_path} успешно загружен")
        except Exception as e:
            logger.error(f"Ошибка загрузки файла {rel_path}: {e}")
            raise CloudStorageError(f"Ошибка загрузки файла: {e}")

    def reload(self, file_path: Path, rel_path: str) -> None:
        """
        Обновляет файл в облачном хранилище с учетом относительного пути.

        Args:
            file_path: Полный путь к локальному файлу.
            rel_path: Относительный путь в облаке.
        """
        self.load(file_path, rel_path)

    def delete(self, file_path: str) -> None:
        """
        Удаляет файл или папку из облачного хранилища.

        Args:
            file_path: Полный путь к файлу/папке в облаке.
        """
        try:
            response = requests.delete(
                f'{self.BASE_URL}/resources',
                headers=self.headers,
                params={'path': file_path, 'permanently': True}
            )
            response.raise_for_status()
            logger.info(f"Объект {file_path} успешно удалён из облака")
        except Exception as e:
            logger.error(f"Ошибка удаления объекта {file_path}: {e}")
            raise CloudStorageError(f"Ошибка удаления объекта: {e}")

    def get_info(self) -> List[Dict[str, Any]]:
        """
        Получает информацию о файлах в облачном хранилище.

        Returns:
            Список словарей с информацией о файлах.
        """
        try:
            response = requests.get(
                f'{self.BASE_URL}/resources',
                headers=self.headers,
                params={'path': f'/{self.cloud_folder}', 'limit': 1000}
            )
            response.raise_for_status()
            data = response.json()
            return data.get('_embedded', {}).get('items', [])
        except Exception as e:
            logger.error(f"Ошибка получения информации о файлах: {e}")
            raise CloudStorageError(f"Ошибка получения информации о файлах: {e}")

    def _get_upload_url(self, rel_path: str) -> str:
        """Получает URL для загрузки файла с учетом относительного пути.
    
            Args:
                rel_path: Относительный путь для сохранения в облаке.
    
            Returns:
                URL для загрузки файла.
    
            Raises:
                CloudStorageError: Если не удалось получить URL для загрузки.
        """
        try:
            response = requests.get(
                f'{self.BASE_URL}/resources/upload',
                headers=self.headers,
                params={'path': f'/{self.cloud_folder}/{rel_path}', 'overwrite': True}
            )
            response.raise_for_status()
            return response.json()['href']
        except Exception as e:
            raise CloudStorageError(f"Ошибка получения URL для загрузки: {e}")

    def create_folder(self, folder_path: str) -> None:
        """
        Создает папку в облачном хранилище.
        Если папка уже существует, просто возвращает управление.

        Args:
            folder_path: Путь к папке относительно корневой папки облака.
        """
        try:
            full_path = f'/{self.cloud_folder}/{folder_path}'
            response = requests.put(
                f'{self.BASE_URL}/resources',
                headers=self.headers,
                params={'path': full_path},
                timeout=10
            )

            # Если папка уже существует (код 409), просто игнорируем это
            if response.status_code == 409:
                return

            response.raise_for_status()
            logger.info(f"Создана папка в облаке: {folder_path}")
        except requests.HTTPError as e:
            if e.response.status_code == 409:
                logger.debug(f"Папка {folder_path} уже существует")
                return
            logger.error(f"Ошибка создания папки {folder_path}: {e}")
            raise CloudStorageError(f"Ошибка создания папки: {e}")
        except Exception as e:
            logger.error(f"Ошибка создания папки {folder_path}: {e}")
            raise CloudStorageError(f"Ошибка создания папки: {e}")

    def get_recursive_info(self) -> List[Dict[str, Any]]:
        """
        Получает рекурсивную информацию о файлах и папках в облачном хранилище.

        Returns:
            Список словарей с информацией о файлах и папках.
        """
        try:
            all_items = []
            stack = [f'/{self.cloud_folder}']

            while stack:
                current_path = stack.pop()
                response = requests.get(
                    f'{self.BASE_URL}/resources',
                    headers=self.headers,
                    params={'path': current_path, 'limit': 1000}
                )
                response.raise_for_status()
                data = response.json()
                items = data.get('_embedded', {}).get('items', [])

                for item in items:
                    all_items.append(item)
                    if item['type'] == 'dir':
                        stack.append(item['path'])

            return all_items
        except Exception as e:
            logger.error(f"Ошибка получения рекурсивной информации: {e}")
            raise CloudStorageError(f"Ошибка получения рекурсивной информации: {e}")

    def rename(self, old_path: str, new_path: str) -> None:
        """Переименовывает файл или папку в облачном хранилище.
    
            Args:
                old_path: Текущий путь к файлу/папке в облаке.
                new_path: Новый путь к файлу/папке в облаке.
    
            Raises:
                CloudStorageError: Если произошла ошибка при переименовании.
        """
        try:
            response = requests.post(
                f'{self.BASE_URL}/resources/move',
                headers=self.headers,
                params={
                    'from': old_path,
                    'path': new_path,
                    'overwrite': True
                },
                timeout=10
            )
            response.raise_for_status()
        except Exception as e:
            raise CloudStorageError(f"Ошибка переименования {old_path} -> {new_path}: {e}")
        