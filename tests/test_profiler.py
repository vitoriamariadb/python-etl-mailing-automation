"""
Testes para modulo de data profiling
"""

import pytest

from etl.profiler import ColumnProfile, DataProfile, DataProfiler


class TestColumnProfile:
    """Testes para perfil de coluna."""

    def test_null_rate(self) -> None:
        """Verifica calculo da taxa de nulos."""
        profile = ColumnProfile(name="col", dtype="int", total_count=10, null_count=3)
        assert profile.null_rate == pytest.approx(0.3)

    def test_null_rate_vazio(self) -> None:
        """Verifica taxa de nulos com dataset vazio."""
        profile = ColumnProfile(name="col", dtype="int", total_count=0, null_count=0)
        assert profile.null_rate == 0.0

    def test_fill_rate(self) -> None:
        """Verifica calculo da taxa de preenchimento."""
        profile = ColumnProfile(name="col", dtype="int", total_count=10, null_count=4)
        assert profile.fill_rate == pytest.approx(0.6)

    def test_unique_rate(self) -> None:
        """Verifica calculo da taxa de unicidade."""
        profile = ColumnProfile(name="col", dtype="str", total_count=20, unique_count=15)
        assert profile.unique_rate == pytest.approx(0.75)

    def test_unique_rate_vazio(self) -> None:
        """Verifica taxa de unicidade com dataset vazio."""
        profile = ColumnProfile(name="col", dtype="str", total_count=0, unique_count=0)
        assert profile.unique_rate == 0.0


class TestDataProfile:
    """Testes para perfil de dataset."""

    def _make_profile(self) -> DataProfile:
        """Cria perfil de teste."""
        return DataProfile(
            source="teste",
            row_count=100,
            column_count=3,
            columns=[
                ColumnProfile(name="id", dtype="int", total_count=100, null_count=0, unique_count=100),
                ColumnProfile(name="nome", dtype="str", total_count=100, null_count=5, unique_count=80),
                ColumnProfile(name="flag", dtype="str", total_count=100, null_count=60, unique_count=2),
            ],
        )

    def test_get_column(self) -> None:
        """Verifica busca de coluna por nome."""
        profile = self._make_profile()
        col = profile.get_column("nome")
        assert col is not None
        assert col.name == "nome"

    def test_get_column_inexistente(self) -> None:
        """Verifica retorno None para coluna inexistente."""
        profile = self._make_profile()
        assert profile.get_column("xyz") is None

    def test_get_high_null_columns(self) -> None:
        """Verifica filtro de colunas com alto percentual de nulos."""
        profile = self._make_profile()
        high_null = profile.get_high_null_columns(threshold=0.5)
        assert len(high_null) == 1
        assert high_null[0].name == "flag"

    def test_to_dict(self) -> None:
        """Verifica serializacao para dicionario."""
        profile = self._make_profile()
        result = profile.to_dict()
        assert result["source"] == "teste"
        assert result["row_count"] == 100
        assert len(result["columns"]) == 3
        assert "null_rate" in result["columns"][0]
        assert "fill_rate" in result["columns"][0]


class TestDataProfiler:
    """Testes para o perfilador de dados."""

    def _sample_data(self) -> list[dict]:
        """Gera dados de exemplo para testes."""
        return [
            {"id": 1, "nome": "Alice", "valor": 100},
            {"id": 2, "nome": "Bob", "valor": 200},
            {"id": 3, "nome": "Carol", "valor": 150},
            {"id": 4, "nome": "David", "valor": 300},
            {"id": 5, "nome": "Eva", "valor": 250},
        ]

    def test_profile_basic(self) -> None:
        """Verifica profiling basico de dataset."""
        profiler = DataProfiler()
        profile = profiler.profile(self._sample_data(), source="teste")
        assert profile.row_count == 5
        assert profile.column_count == 3
        assert len(profile.columns) == 3

    def test_profile_numeric_stats(self) -> None:
        """Verifica estatisticas numericas no perfil."""
        profiler = DataProfiler()
        profile = profiler.profile(self._sample_data(), source="teste")
        col_valor = profile.get_column("valor")
        assert col_valor is not None
        assert col_valor.min_value == 100.0
        assert col_valor.max_value == 300.0
        assert col_valor.mean_value == pytest.approx(200.0)
        assert col_valor.median_value == pytest.approx(200.0)

    def test_profile_string_columns(self) -> None:
        """Verifica perfil de colunas textuais."""
        profiler = DataProfiler()
        profile = profiler.profile(self._sample_data(), source="teste")
        col_nome = profile.get_column("nome")
        assert col_nome is not None
        assert col_nome.dtype == "str"
        assert col_nome.unique_count == 5

    def test_profile_empty_dataset(self) -> None:
        """Verifica profiling de dataset vazio."""
        profiler = DataProfiler()
        profile = profiler.profile([], source="vazio")
        assert profile.row_count == 0
        assert profile.column_count == 0
        assert len(profile.columns) == 0

    def test_warnings_high_null(self) -> None:
        """Verifica aviso para colunas com muitos nulos."""
        data = [
            {"id": 1, "flag": None},
            {"id": 2, "flag": None},
            {"id": 3, "flag": None},
            {"id": 4, "flag": "X"},
        ]
        profiler = DataProfiler()
        profile = profiler.profile(data, source="teste_null")
        null_warnings = [w for w in profile.warnings if "nulos" in w]
        assert len(null_warnings) == 1
        assert "flag" in null_warnings[0]

    def test_warnings_constant_value(self) -> None:
        """Verifica aviso para colunas com valor constante."""
        data = [
            {"id": 1, "status": "ativo"},
            {"id": 2, "status": "ativo"},
            {"id": 3, "status": "ativo"},
        ]
        profiler = DataProfiler()
        profile = profiler.profile(data, source="teste_constante")
        const_warnings = [w for w in profile.warnings if "constante" in w]
        assert len(const_warnings) == 1
        assert "status" in const_warnings[0]

# "Aquele que tem um porque para viver pode suportar quase qualquer como." -- Friedrich Nietzsche

