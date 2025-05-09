class ConfigError(Exception):
    """Ошибка конфигурации."""
    pass

class CloudStorageError(Exception):
    """Ошибка облачного хранилища."""
    pass

class SyncError(Exception):
    """Ошибка синхронизации."""
    pass