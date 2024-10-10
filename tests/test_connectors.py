"""
Testes para conectores de dados
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from etl.connectors import DuckDBConnector, ConnectorFactory, FileConnector, BaseConnector


class TestDuckDBConnector:
    """Testes para o conector DuckDB."""

    def test_connect_memory(self) -> None:
        """Verifica conexao padrao em memoria."""
        mock_duckdb = MagicMock()
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            connector = DuckDBConnector()
            connector.connect()

        mock_duckdb.connect.assert_called_once_with(":memory:")
        assert connector._connection is mock_conn

    def test_read_returns_dataframe(self) -> None:
        """Verifica que read retorna DataFrame com colunas corretas."""
        mock_duckdb = MagicMock()
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        mock_result = MagicMock()
        mock_result.description = [("id",), ("nome",)]
        mock_result.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        mock_conn.execute.return_value = mock_result

        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            connector = DuckDBConnector()
            connector.connect()
            df = connector.read("SELECT * FROM tabela")

        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ["id", "nome"]
        assert len(df) == 2

    def test_write_inserts_data(self) -> None:
        """Verifica insercao de DataFrame em tabela."""
        mock_duckdb = MagicMock()
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            connector = DuckDBConnector()
            connector.connect()
            df = pd.DataFrame({"id": [1, 2], "valor": [10, 20]})
            result = connector.write(df, "tabela_destino")

        assert result is True
        mock_conn.register.assert_called_once()
        mock_conn.execute.assert_called_once()
        mock_conn.unregister.assert_called_once_with("_temp_df")

    def test_execute_ddl(self) -> None:
        """Verifica execucao de comandos DDL."""
        mock_duckdb = MagicMock()
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            connector = DuckDBConnector()
            connector.connect()
            connector.execute("CREATE TABLE t (id INT)")

        mock_conn.execute.assert_called_once_with("CREATE TABLE t (id INT)")

    def test_read_file_csv(self) -> None:
        """Verifica leitura de arquivo CSV via DuckDB."""
        mock_duckdb = MagicMock()
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        mock_result = MagicMock()
        mock_result.description = [("col1",)]
        mock_result.fetchall.return_value = [("valor1",)]
        mock_conn.execute.return_value = mock_result

        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            connector = DuckDBConnector()
            connector.connect()
            df = connector.read_file("/tmp/dados.csv", format="csv")

        assert isinstance(df, pd.DataFrame)
        call_args = mock_conn.execute.call_args[0][0]
        assert "read_csv_auto" in call_args

    def test_read_file_formato_invalido(self) -> None:
        """Verifica erro ao usar formato nao suportado."""
        mock_duckdb = MagicMock()
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            connector = DuckDBConnector()
            connector.connect()
            with pytest.raises(ValueError, match="Formato nao suportado"):
                connector.read_file("/tmp/dados.xyz", format="xyz")

    def test_context_manager(self) -> None:
        """Verifica protocolo de context manager."""
        mock_duckdb = MagicMock()
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            with DuckDBConnector() as conn:
                assert conn._connection is mock_conn
            mock_conn.close.assert_called_once()

    def test_disconnect(self) -> None:
        """Verifica desconexao limpa."""
        mock_duckdb = MagicMock()
        mock_conn = MagicMock()
        mock_duckdb.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            connector = DuckDBConnector()
            connector.connect()
            connector.disconnect()

        mock_conn.close.assert_called_once()
        assert connector._connection is None

    def test_disconnect_sem_conexao(self) -> None:
        """Verifica que disconnect sem conexao nao causa erro."""
        connector = DuckDBConnector()
        connector.disconnect()
        assert connector._connection is None


class TestConnectorFactory:
    """Testes para a factory de conectores."""

    def test_create_file_connector(self) -> None:
        """Verifica criacao de conector de arquivo."""
        connector = ConnectorFactory.create("csv")
        assert isinstance(connector, FileConnector)

    def test_create_duckdb_connector(self) -> None:
        """Verifica criacao de conector DuckDB."""
        mock_duckdb = MagicMock()
        with patch.dict("sys.modules", {"duckdb": mock_duckdb}):
            connector = ConnectorFactory.create("duckdb")
        assert isinstance(connector, DuckDBConnector)

    def test_register_custom_connector(self) -> None:
        """Verifica registro de conector customizado."""

        class CustomConnector(BaseConnector):
            def read(self, source, **kwargs):
                return pd.DataFrame()

            def write(self, df, destination, **kwargs):
                return True

        ConnectorFactory.register("custom", CustomConnector)
        connector = ConnectorFactory.create("custom")
        assert isinstance(connector, CustomConnector)

    def test_create_tipo_invalido(self) -> None:
        """Verifica erro ao criar conector de tipo desconhecido."""
        with pytest.raises(ValueError, match="nao suportado"):
            ConnectorFactory.create("inexistente")


# "Nao e porque as coisas sao dificeis que nao ousamos; e porque nao ousamos que sao dificeis." -- Seneca

