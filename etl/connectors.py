"""
Conectores para diferentes fontes de dados
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

class BaseConnector(ABC):
    """Classe base para conectores de dados"""

    @abstractmethod
    def read(self, source: str | Path, **kwargs) -> pd.DataFrame:
        """Le dados da fonte"""
        pass

    @abstractmethod
    def write(self, df: pd.DataFrame, destination: str | Path, **kwargs) -> bool:
        """Escreve dados no destino"""
        pass


class FileConnector(BaseConnector):
    """Conector generico para arquivos"""

    def __init__(self, file_type: str) -> None:
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

    def read(self, source: str | Path, **kwargs) -> pd.DataFrame:
        """Le dados de arquivo"""
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"Arquivo nao encontrado: {source}")

        reader = self.readers.get(self.file_type)
        if not reader:
            raise ValueError(f"Tipo de arquivo nao suportado: {self.file_type}")

        return reader(path, **kwargs)

    def write(self, df: pd.DataFrame, destination: str | Path, **kwargs) -> bool:
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


class DuckDBConnector(BaseConnector):
    """Conector para DuckDB - analytics local de alto desempenho."""

    def __init__(self, database: str | Path = ":memory:") -> None:
        self._database = str(database)
        self._connection = None

    def connect(self) -> None:
        """Estabelece conexao com DuckDB."""
        try:
            import duckdb
        except ImportError:
            logger.error("duckdb nao instalado: pip install duckdb")
            raise
        self._connection = duckdb.connect(self._database)
        logger.info("Conectado ao DuckDB: %s", self._database)

    def disconnect(self) -> None:
        """Fecha conexao com DuckDB."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Desconectado do DuckDB")

    def read(self, query: str, **kwargs) -> pd.DataFrame:
        """Executa query e retorna resultados como DataFrame."""
        if not self._connection:
            self.connect()
        result = self._connection.execute(query)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return pd.DataFrame(rows, columns=columns)

    def write(self, df: pd.DataFrame, destination: str | Path, **kwargs) -> bool:
        """Insere DataFrame em uma tabela DuckDB."""
        if not self._connection:
            self.connect()
        table = str(destination)
        self._connection.register("_temp_df", df)
        self._connection.execute(f"INSERT INTO {table} SELECT * FROM _temp_df")
        self._connection.unregister("_temp_df")
        logger.info("Inseridos %d registros em %s", len(df), table)
        return True

    def execute(self, query: str) -> None:
        """Executa comando DDL/DML sem retorno."""
        if not self._connection:
            self.connect()
        self._connection.execute(query)

    def read_file(self, filepath: str | Path, format: str = "csv") -> pd.DataFrame:
        """Le arquivo diretamente via DuckDB (CSV, Parquet, JSON)."""
        if not self._connection:
            self.connect()
        filepath = str(filepath)
        query_map = {
            "csv": f"SELECT * FROM read_csv_auto('{filepath}')",
            "parquet": f"SELECT * FROM read_parquet('{filepath}')",
            "json": f"SELECT * FROM read_json_auto('{filepath}')",
        }
        query = query_map.get(format)
        if not query:
            raise ValueError(f"Formato nao suportado: {format}")
        return self.read(query)

    def __enter__(self) -> "DuckDBConnector":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()


class ConnectorFactory:
    """Factory para criacao de conectores"""

    _connectors: dict[str, type] = {
        "duckdb": DuckDBConnector,
    }

    @staticmethod
    def create(connector_type: str, **kwargs) -> BaseConnector:
        """Cria conector baseado no tipo"""
        file_types = ['csv', 'json', 'excel', 'parquet']

        if connector_type in file_types:
            return FileConnector(connector_type)

        if connector_type in ConnectorFactory._connectors:
            return ConnectorFactory._connectors[connector_type](**kwargs)

        raise ValueError(f"Tipo de conector nao suportado: {connector_type}")

    @classmethod
    def register(cls, name: str, connector_class: type) -> None:
        """Registra novo tipo de conector."""
        cls._connectors[name] = connector_class
        logger.info("Conector registrado: %s", name)


# "A riqueza consiste muito mais no desfrute do que na posse." -- Aristoteles

