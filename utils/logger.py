import logging
from typing import Any


def setup_logger(name: str) -> Any:
    """
    Настраивает и возвращает логгер.

    Args:
        name: Имя логгера.

    Returns:
        Настроенный логгер.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Консольный вывод
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
