"""
Conectores para diferentes fontes de dados
"""

from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
from typing import Union, Dict, Any, Optional


class BaseConnector(ABC):
    """Classe base para conectores de dados"""

    @abstractmethod
    def read(self, source: Union[str, Path], **kwargs) -> pd.DataFrame:
        """Le dados da fonte"""
        pass

    @abstractmethod
    def write(self, df: pd.DataFrame, destination: Union[str, Path], **kwargs) -> bool:
        """Escreve dados no destino"""
        pass


class FileConnector(BaseConnector):
    """Conector generico para arquivos"""

    def __init__(self, file_type: str):
        self.file_type = file_type
        self.readers = {
            'csv': pd.read_csv,
            'json': pd.read_json,
            'excel': pd.read_excel,
            'parquet': pd.read_parquet
        }
        self.writers = {
            'csv': lambda df, path, **kw: df.to_csv(path, index=False, **kw),
            'json': lambda df, path, **kw: df.to_json(path, orient='records', **kw),
            'excel': lambda df, path, **kw: df.to_excel(path, index=False, **kw),
            'parquet': lambda df, path, **kw: df.to_parquet(path, **kw)
        }

    def read(self, source: Union[str, Path], **kwargs) -> pd.DataFrame:
        """Le dados de arquivo"""
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"Arquivo nao encontrado: {source}")

        reader = self.readers.get(self.file_type)
        if not reader:
            raise ValueError(f"Tipo de arquivo nao suportado: {self.file_type}")

        return reader(path, **kwargs)

    def write(self, df: pd.DataFrame, destination: Union[str, Path], **kwargs) -> bool:
        """Escreve dados em arquivo"""
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)

        writer = self.writers.get(self.file_type)
        if not writer:
            raise ValueError(f"Tipo de arquivo nao suportado: {self.file_type}")

        try:
            writer(df, path, **kwargs)
            return True
        except Exception as e:
            raise RuntimeError(f"Erro ao escrever arquivo {self.file_type}: {e}")


class ConnectorFactory:
    """Factory para criacao de conectores"""

    @staticmethod
    def create(connector_type: str, **kwargs) -> BaseConnector:
        """Cria conector baseado no tipo"""
        file_types = ['csv', 'json', 'excel', 'parquet']

        if connector_type in file_types:
            return FileConnector(connector_type)

        raise ValueError(f"Tipo de conector nao suportado: {connector_type}")
