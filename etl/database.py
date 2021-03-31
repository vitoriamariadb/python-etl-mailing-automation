"""
Conectores para bancos de dados
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any, Optional, List
from contextlib import contextmanager

try:
    import psycopg2
except ImportError:
    psycopg2 = None

try:
    import pymysql
except ImportError:
    pymysql = None

try:
    from sqlalchemy import create_engine
except ImportError:
    create_engine = None


class DatabaseConnector(ABC):
    """Classe base para conectores de banco de dados"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = None

    @abstractmethod
    def connect(self):
        """Estabelece conexao com banco"""
        pass

    @abstractmethod
    def disconnect(self):
        """Fecha conexao com banco"""
        pass

    @abstractmethod
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Executa query e retorna DataFrame"""
        pass

    @abstractmethod
    def insert_dataframe(self, df: pd.DataFrame, table: str, if_exists: str = 'append') -> bool:
        """Insere DataFrame em tabela"""
        pass

    @contextmanager
    def transaction(self):
        """Context manager para transacoes"""
        try:
            self.connect()
            yield self
        finally:
            self.disconnect()


class PostgresConnector(DatabaseConnector):
    """Conector para PostgreSQL"""

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        super().__init__(connection_string)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        """Conecta ao PostgreSQL"""
        if psycopg2 is None:
            raise ImportError("psycopg2 nao instalado. Execute: pip install psycopg2-binary")
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
        except Exception as e:
            raise ConnectionError(f"Erro ao conectar PostgreSQL: {e}")

    def disconnect(self):
        """Desconecta do PostgreSQL"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Executa query SQL"""
        if not self.connection:
            self.connect()

        try:
            return pd.read_sql_query(query, self.connection, params=params)
        except Exception as e:
            raise RuntimeError(f"Erro ao executar query: {e}")

    def insert_dataframe(self, df: pd.DataFrame, table: str, if_exists: str = 'append') -> bool:
        """Insere DataFrame em tabela PostgreSQL"""
        if not self.connection:
            self.connect()

        if create_engine is None:
            raise ImportError("sqlalchemy nao instalado. Execute: pip install sqlalchemy")

        try:
            engine = create_engine(self.connection_string)
            df.to_sql(table, engine, if_exists=if_exists, index=False)
            return True
        except Exception as e:
            raise RuntimeError(f"Erro ao inserir dados: {e}")


class MySQLConnector(DatabaseConnector):
    """Conector para MySQL"""

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        super().__init__(connection_string)
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        """Conecta ao MySQL"""
        if pymysql is None:
            raise ImportError("pymysql nao instalado. Execute: pip install pymysql")
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
        except Exception as e:
            raise ConnectionError(f"Erro ao conectar MySQL: {e}")

    def disconnect(self):
        """Desconecta do MySQL"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Executa query SQL"""
        if not self.connection:
            self.connect()

        try:
            return pd.read_sql_query(query, self.connection, params=params)
        except Exception as e:
            raise RuntimeError(f"Erro ao executar query: {e}")

    def insert_dataframe(self, df: pd.DataFrame, table: str, if_exists: str = 'append') -> bool:
        """Insere DataFrame em tabela MySQL"""
        if not self.connection:
            self.connect()

        if create_engine is None:
            raise ImportError("sqlalchemy nao instalado. Execute: pip install sqlalchemy")

        try:
            engine = create_engine(self.connection_string)
            df.to_sql(table, engine, if_exists=if_exists, index=False)
            return True
        except Exception as e:
            raise RuntimeError(f"Erro ao inserir dados: {e}")


class DatabaseFactory:
    """Factory para criacao de conectores de banco"""

    @staticmethod
    def create(db_type: str, config: Dict[str, Any]) -> DatabaseConnector:
        """
        Cria conector de banco de dados

        Args:
            db_type: Tipo do banco (postgres, mysql)
            config: Configuracao de conexao

        Returns:
            Conector de banco de dados
        """
        if db_type == 'postgres':
            return PostgresConnector(**config)
        elif db_type == 'mysql':
            return MySQLConnector(**config)
        else:
            raise ValueError(f"Tipo de banco nao suportado: {db_type}")
