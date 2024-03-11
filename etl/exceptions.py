"""
Excecoes customizadas para pipeline ETL
"""

class ETLBaseException(Exception):
    """Excecao base para pipeline ETL"""

    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

class ExtractionError(ETLBaseException):
    """Erro durante extracao de dados"""
    pass

class TransformationError(ETLBaseException):
    """Erro durante transformacao de dados"""
    pass

class LoadError(ETLBaseException):
    """Erro durante carga de dados"""
    pass

class ValidationError(ETLBaseException):
    """Erro de validacao de dados"""
    pass

class ConfigurationError(ETLBaseException):
    """Erro de configuracao"""
    pass

class DataQualityError(ETLBaseException):
    """Erro de qualidade de dados"""
    pass

