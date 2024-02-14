"""
Sistema de error handling para pipeline ETL
"""

import traceback
from typing import Callable, Any
from functools import wraps

from .exceptions import ETLBaseException, ExtractionError, TransformationError, LoadError
from .logger import get_logger

logger = get_logger('error_handler')

class ErrorHandler:
    """Handler centralizado de erros para pipeline ETL"""

    def __init__(self, raise_on_error: bool = True):
        self.raise_on_error = raise_on_error
        self.errors = []

    def handle_error(self, error: Exception, context: str = '', reraise: bool = None) -> Exception | None:
        """
        Trata erro e registra

        Args:
            error: Excecao capturada
            context: Contexto do erro
            reraise: Se deve re-lancar a excecao

        Returns:
            Exception se nao re-lancada
        """
        should_reraise = reraise if reraise is not None else self.raise_on_error

        error_info = {
            'type': type(error).__name__,
            'message': str(error),
            'context': context,
            'traceback': traceback.format_exc()
        }

        self.errors.append(error_info)

        logger.error(f"Erro no contexto '{context}': {error}", exc_info=True)

        if should_reraise:
            raise error

        return error

    def get_error_summary(self) -> dict[str, Any]:
        """
        Retorna resumo de erros

        Returns:
            Dicionario com resumo
        """
        return {
            'total_errors': len(self.errors),
            'errors_by_type': self._group_errors_by_type(),
            'recent_errors': self.errors[-5:] if self.errors else []
        }

    def _group_errors_by_type(self) -> dict[str, int]:
        """Agrupa erros por tipo"""
        error_types = {}
        for error in self.errors:
            error_type = error['type']
            error_types[error_type] = error_types.get(error_type, 0) + 1
        return error_types

    def clear_errors(self):
        """Limpa historico de erros"""
        self.errors = []

def handle_etl_errors(context: str = '', error_type: type = ETLBaseException):
    """
    Decorator para tratamento automatico de erros

    Args:
        context: Contexto da operacao
        error_type: Tipo de excecao a lancar
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except ETLBaseException:
                raise
            except Exception as e:
                logger.error(f"Erro em {context or func.__name__}: {e}", exc_info=True)
                raise error_type(f"Erro em {context or func.__name__}: {str(e)}", {'original_error': str(e)})
        return wrapper
    return decorator

def safe_execute(func: Callable, *args, default_value: Any = None, logger_instance: Any = None, **kwargs) -> Any:
    """
    Executa funcao com tratamento de erro

    Args:
        func: Funcao a executar
        *args: Argumentos posicionais
        default_value: Valor padrao em caso de erro
        logger_instance: Logger customizado
        **kwargs: Argumentos nomeados

    Returns:
        Resultado da funcao ou default_value
    """
    log = logger_instance or logger

    try:
        return func(*args, **kwargs)
    except Exception as e:
        log.error(f"Erro ao executar {func.__name__}: {e}", exc_info=True)
        return default_value

