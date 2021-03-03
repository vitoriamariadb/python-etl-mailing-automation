"""
Testes para conectores de banco de dados com mocks
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

from etl.database import PostgresConnector, MySQLConnector, DatabaseFactory


class TestDatabaseConnectors:
    """Testes para conectores de banco de dados"""

    @pytest.fixture
    def sample_dataframe(self):
        """DataFrame de exemplo para testes"""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300]
        })

    @pytest.fixture
    def postgres_config(self):
        """Configuracao PostgreSQL para testes"""
        return {
            'host': 'localhost',
            'port': 5432,
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }

    @pytest.fixture
    def mysql_config(self):
        """Configuracao MySQL para testes"""
        return {
            'host': 'localhost',
            'port': 3306,
            'database': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }

    @patch('etl.database.psycopg2')
    def test_postgres_connect(self, mock_psycopg2, postgres_config):
        """Testa conexao PostgreSQL"""
        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        connector = PostgresConnector(**postgres_config)
        connector.connect()

        mock_psycopg2.connect.assert_called_once_with(
            host='localhost',
            port=5432,
            database='testdb',
            user='testuser',
            password='testpass'
        )

        assert connector.connection == mock_conn

    @patch('etl.database.psycopg2')
    def test_postgres_disconnect(self, mock_psycopg2, postgres_config):
        """Testa desconexao PostgreSQL"""
        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        connector = PostgresConnector(**postgres_config)
        connector.connect()
        connector.disconnect()

        mock_conn.close.assert_called_once()
        assert connector.connection is None

    @patch('etl.database.pd.read_sql_query')
    @patch('etl.database.psycopg2')
    def test_postgres_execute_query(self, mock_psycopg2, mock_read_sql, postgres_config, sample_dataframe):
        """Testa execucao de query PostgreSQL"""
        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_read_sql.return_value = sample_dataframe

        connector = PostgresConnector(**postgres_config)
        connector.connect()

        result = connector.execute_query("SELECT * FROM test_table")

        mock_read_sql.assert_called_once()
        assert result.equals(sample_dataframe)

    @patch('etl.database.create_engine')
    @patch('etl.database.psycopg2')
    def test_postgres_insert_dataframe(self, mock_psycopg2, mock_create_engine, postgres_config, sample_dataframe):
        """Testa insercao de DataFrame PostgreSQL"""
        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        connector = PostgresConnector(**postgres_config)
        connector.connect()

        with patch.object(sample_dataframe, 'to_sql') as mock_to_sql:
            result = connector.insert_dataframe(sample_dataframe, 'test_table')

            assert result is True
            mock_to_sql.assert_called_once_with('test_table', mock_engine, if_exists='append', index=False)

    @patch('etl.database.pymysql')
    def test_mysql_connect(self, mock_pymysql, mysql_config):
        """Testa conexao MySQL"""
        mock_conn = MagicMock()
        mock_pymysql.connect.return_value = mock_conn

        connector = MySQLConnector(**mysql_config)
        connector.connect()

        mock_pymysql.connect.assert_called_once_with(
            host='localhost',
            port=3306,
            database='testdb',
            user='testuser',
            password='testpass'
        )

        assert connector.connection == mock_conn

    @patch('etl.database.pymysql')
    def test_mysql_disconnect(self, mock_pymysql, mysql_config):
        """Testa desconexao MySQL"""
        mock_conn = MagicMock()
        mock_pymysql.connect.return_value = mock_conn

        connector = MySQLConnector(**mysql_config)
        connector.connect()
        connector.disconnect()

        mock_conn.close.assert_called_once()
        assert connector.connection is None

    @patch('etl.database.pd.read_sql_query')
    @patch('etl.database.pymysql')
    def test_mysql_execute_query(self, mock_pymysql, mock_read_sql, mysql_config, sample_dataframe):
        """Testa execucao de query MySQL"""
        mock_conn = MagicMock()
        mock_pymysql.connect.return_value = mock_conn
        mock_read_sql.return_value = sample_dataframe

        connector = MySQLConnector(**mysql_config)
        connector.connect()

        result = connector.execute_query("SELECT * FROM test_table")

        mock_read_sql.assert_called_once()
        assert result.equals(sample_dataframe)

    def test_database_factory_postgres(self, postgres_config):
        """Testa factory para PostgreSQL"""
        connector = DatabaseFactory.create('postgres', postgres_config)
        assert isinstance(connector, PostgresConnector)

    def test_database_factory_mysql(self, mysql_config):
        """Testa factory para MySQL"""
        connector = DatabaseFactory.create('mysql', mysql_config)
        assert isinstance(connector, MySQLConnector)

    def test_database_factory_invalid_type(self):
        """Testa factory com tipo invalido"""
        with pytest.raises(ValueError, match="Tipo de banco nao suportado"):
            DatabaseFactory.create('invalid', {})

    @patch('etl.database.psycopg2')
    def test_postgres_transaction_context(self, mock_psycopg2, postgres_config):
        """Testa context manager de transacao PostgreSQL"""
        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        connector = PostgresConnector(**postgres_config)

        with connector.transaction():
            pass

        mock_psycopg2.connect.assert_called_once()
        mock_conn.close.assert_called_once()
