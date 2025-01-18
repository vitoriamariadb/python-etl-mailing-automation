"""
Sistema de retry para operacoes ETL
"""

import time
from typing import Callable, Any, Type
from functools import wraps

from .logger import get_logger

logger = get_logger('retry')

class RetryConfig:
    """Configuracao de retry"""

    def __init__(self,
                 max_attempts: int = 3,
                 delay: float = 1.0,
                 backoff_factor: float = 2.0,
                 max_delay: float = 60.0,
                 exceptions: tuple[Type[Exception], ...] = (Exception,)):
        self.max_attempts = max_attempts
        self.delay = delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
        self.exceptions = exceptions

def retry_operation(config: RetryConfig | None = None):
    """
    Decorator para retry automatico de operacoes

    Args:
        config: Configuracao de retry
    """
    if config is None:
        config = RetryConfig()

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            current_delay = config.delay

            for attempt in range(1, config.max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 1:
                        logger.info(f"{func.__name__} sucesso na tentativa {attempt}")
                    return result

                except config.exceptions as e:
                    last_exception = e

                    if attempt == config.max_attempts:
                        logger.error(f"{func.__name__} falhou apos {attempt} tentativas: {e}")
                        raise

                    logger.warning(f"{func.__name__} falhou na tentativa {attempt}/{config.max_attempts}: {e}. Retentando em {current_delay}s...")

                    time.sleep(current_delay)
                    current_delay = min(current_delay * config.backoff_factor, config.max_delay)

            raise last_exception

        return wrapper
    return decorator

class RetryManager:
    """Gerenciador de retry para operacoes"""

    def __init__(self, config: RetryConfig | None = None):
        self.config = config or RetryConfig()
        self.retry_stats = {
            'total_operations': 0,
            'successful_first_try': 0,
            'successful_with_retry': 0,
            'failed': 0
        }

    def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """
        Executa funcao com retry

        Args:
            func: Funcao a executar
            *args: Argumentos posicionais
            **kwargs: Argumentos nomeados

        Returns:
            Resultado da funcao
        """
        self.retry_stats['total_operations'] += 1
        last_exception = None
        current_delay = self.config.delay

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                result = func(*args, **kwargs)

                if attempt == 1:
                    self.retry_stats['successful_first_try'] += 1
                else:
                    self.retry_stats['successful_with_retry'] += 1
                    logger.info(f"{func.__name__} sucesso na tentativa {attempt}")

                return result

            except self.config.exceptions as e:
                last_exception = e

                if attempt == self.config.max_attempts:
                    self.retry_stats['failed'] += 1
                    logger.error(f"{func.__name__} falhou apos {attempt} tentativas: {e}")
                    raise

                logger.warning(f"{func.__name__} falhou na tentativa {attempt}/{self.config.max_attempts}: {e}. Retentando em {current_delay}s...")

                time.sleep(current_delay)
                current_delay = min(current_delay * self.config.backoff_factor, self.config.max_delay)

        raise last_exception

    def get_stats(self) -> dict:
        """
        Retorna estatisticas de retry

        Returns:
            Dicionario com estatisticas
        """
        total = self.retry_stats['total_operations']
        if total == 0:
            return self.retry_stats

        return {
            **self.retry_stats,
            'success_rate': (self.retry_stats['successful_first_try'] + self.retry_stats['successful_with_retry']) / total * 100,
            'retry_rate': self.retry_stats['successful_with_retry'] / total * 100 if total > 0 else 0
        }

    def reset_stats(self):
        """Reseta estatisticas"""
        self.retry_stats = {
            'total_operations': 0,
            'successful_first_try': 0,
            'successful_with_retry': 0,
            'failed': 0
        }

def exponential_backoff(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0) -> float:
    """
    Calcula delay com exponential backoff

    Args:
        attempt: Numero da tentativa
        base_delay: Delay base
        max_delay: Delay maximo

    Returns:
        Delay calculado
    """
    delay = base_delay * (2 ** (attempt - 1))
    return min(delay, max_delay)
