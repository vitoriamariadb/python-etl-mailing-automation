"""
Sistema de logging estruturado para pipeline ETL
"""

import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler

class ETLLogger:
    """Logger estruturado para operacoes ETL"""

    def __init__(self, name: str = 'etl', log_dir: Path | None = None, level: int = logging.INFO):
        self.name = name
        self.log_dir = log_dir or Path('logs')
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.level = level
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """
        Configura logger com handlers para arquivo e console

        Returns:
            Logger configurado
        """
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)

        if logger.handlers:
            return logger

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        file_handler = RotatingFileHandler(
            self.log_dir / f'{self.name}.log',
            maxBytes=10*1024*1024,
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(self.level)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    def info(self, message: str, **kwargs):
        """Log nivel INFO"""
        self.logger.info(message, extra=kwargs)

    def warning(self, message: str, **kwargs):
        """Log nivel WARNING"""
        self.logger.warning(message, extra=kwargs)

    def error(self, message: str, **kwargs):
        """Log nivel ERROR"""
        self.logger.error(message, extra=kwargs)

    def debug(self, message: str, **kwargs):
        """Log nivel DEBUG"""
        self.logger.debug(message, extra=kwargs)

    def critical(self, message: str, **kwargs):
        """Log nivel CRITICAL"""
        self.logger.critical(message, extra=kwargs)

    def log_extraction(self, source: str, rows: int, status: str = 'success'):
        """Log especifico para extracao"""
        self.info(f"Extracao - Fonte: {source} | Linhas: {rows} | Status: {status}")

    def log_transformation(self, operation: str, rows_before: int, rows_after: int, status: str = 'success'):
        """Log especifico para transformacao"""
        self.info(f"Transformacao - Operacao: {operation} | Antes: {rows_before} | Depois: {rows_after} | Status: {status}")

    def log_load(self, destination: str, rows: int, status: str = 'success'):
        """Log especifico para carga"""
        self.info(f"Carga - Destino: {destination} | Linhas: {rows} | Status: {status}")

    def log_pipeline_execution(self, pipeline_name: str, duration: float, rows: int, status: str = 'success'):
        """Log especifico para execucao de pipeline"""
        self.info(f"Pipeline: {pipeline_name} | Duracao: {duration:.2f}s | Linhas: {rows} | Status: {status}")

def get_logger(name: str = 'etl', log_dir: Path | None = None, level: int = logging.INFO) -> ETLLogger:
    """
    Factory function para criar logger

    Args:
        name: Nome do logger
        log_dir: Diretorio de logs
        level: Nivel de logging

    Returns:
        Instancia de ETLLogger
    """
    return ETLLogger(name, log_dir, level)

