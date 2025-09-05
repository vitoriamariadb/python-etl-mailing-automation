"""
Testes de edge cases para validadores de dados
"""

import numpy as np
import pandas as pd
import pytest

from etl.validators import DataValidator


class TestValidatorEdgeCases:
    """Testes para casos limite dos validadores."""

    def setup_method(self) -> None:
        """Inicializa validador para cada teste."""
        self.validator = DataValidator()

    def test_validate_not_null_empty_string(self) -> None:
        """String vazia nao e nula para pandas."""
        df = pd.DataFrame({"col": ["", "a", "b"]})
        result = self.validator.validate_not_null(df, ["col"])
        assert result["passed"] is True

    def test_validate_not_null_whitespace_only(self) -> None:
        """Espacos em branco nao sao nulos para pandas."""
        df = pd.DataFrame({"col": ["   ", "\t", "\n"]})
        result = self.validator.validate_not_null(df, ["col"])
        assert result["passed"] is True

    def test_validate_not_null_zero_is_valid(self) -> None:
        """Zero numerico nao e nulo."""
        df = pd.DataFrame({"col": [0, 0, 0]})
        result = self.validator.validate_not_null(df, ["col"])
        assert result["passed"] is True

    def test_validate_not_null_false_is_valid(self) -> None:
        """Boolean False nao e nulo."""
        df = pd.DataFrame({"col": [False, False, True]})
        result = self.validator.validate_not_null(df, ["col"])
        assert result["passed"] is True

    def test_validate_not_null_nested_none(self) -> None:
        """None dentro de lista e armazenado como object, nao como NaN."""
        df = pd.DataFrame({"col": [[None], [1, 2], [3]]})
        result = self.validator.validate_not_null(df, ["col"])
        assert result["passed"] is True

    def test_validate_unique_single_element(self) -> None:
        """Elemento unico sempre e unico."""
        df = pd.DataFrame({"col": [42]})
        result = self.validator.validate_unique(df, ["col"])
        assert result["passed"] is True

    def test_validate_unique_all_none(self) -> None:
        """Multiplos NaN sao tratados como duplicatas pelo pandas."""
        df = pd.DataFrame({"col": [None, None, None]})
        result = self.validator.validate_unique(df, ["col"])
        assert result["passed"] is False
        assert len(result["errors"]) > 0

    def test_validate_unique_mixed_types(self) -> None:
        """Tipos mistos numa coluna object nao causam erro."""
        df = pd.DataFrame({"col": [1, "1", 1.0, True]})
        result = self.validator.validate_unique(df, ["col"])
        assert "rule" in result
        assert isinstance(result["passed"], bool)

    def test_validate_data_type_bool_vs_int(self) -> None:
        """Bool e subclasse de int em Python, mas pandas distingue os dtypes."""
        df = pd.DataFrame({"flag": [True, False, True]})
        result = self.validator.validate_data_type(df, {"flag": "bool"})
        assert result["passed"] is True

    def test_validate_data_type_none_values(self) -> None:
        """Coluna com None vira float64 no pandas (NaN e float)."""
        df = pd.DataFrame({"col": [1, None, 3]})
        result = self.validator.validate_data_type(df, {"col": "int"})
        assert result["passed"] is False
        assert any("esperado" in e for e in result["errors"])

    def test_validate_range_with_none_bounds(self) -> None:
        """Sem limites definidos, qualquer valor e aceito."""
        df = pd.DataFrame({"col": [-1000, 0, 1000]})
        result = self.validator.validate_range(df, "col", min_value=None, max_value=None)
        assert result["passed"] is True

    def test_validate_range_negative_values(self) -> None:
        """Validacao de range com valores negativos."""
        df = pd.DataFrame({"col": [-10, -5, -1]})
        result = self.validator.validate_range(df, "col", min_value=-10, max_value=-1)
        assert result["passed"] is True

    def test_validate_range_equal_min_max(self) -> None:
        """Min e max iguais aceita apenas aquele valor exato."""
        df = pd.DataFrame({"col": [5, 5, 5]})
        result = self.validator.validate_range(df, "col", min_value=5, max_value=5)
        assert result["passed"] is True

    def test_validate_range_equal_min_max_with_outlier(self) -> None:
        """Min e max iguais rejeita valores diferentes."""
        df = pd.DataFrame({"col": [5, 6, 5]})
        result = self.validator.validate_range(df, "col", min_value=5, max_value=5)
        assert result["passed"] is False

    def test_validate_custom_exception_in_rule(self) -> None:
        """Excecao na funcao customizada e capturada sem propagacao."""
        df = pd.DataFrame({"col": [1, 2, 3]})

        def regra_com_erro(dataframe: pd.DataFrame) -> bool:
            raise RuntimeError("erro proposital")

        result = self.validator.validate_custom(df, "regra_erro", regra_com_erro, "falhou")
        assert result["passed"] is False
        assert any("Erro na validacao" in e for e in result["errors"])

    def test_validate_all_empty_rules(self) -> None:
        """Lista vazia de regras retorna resultado valido."""
        df = pd.DataFrame({"col": [1]})
        result = self.validator.validate_all(df, [])
        assert result["total_rules"] == 0
        assert result["passed"] == 0
        assert result["failed"] == 0

    def test_validate_all_partial_failure(self) -> None:
        """Falha parcial reporta total correto de aprovados e reprovados."""
        df = pd.DataFrame({"id": [1, 2, 3], "nome": ["a", None, "c"]})
        rules = [
            {"type": "not_null", "columns": ["id"]},
            {"type": "not_null", "columns": ["nome"]},
        ]
        result = self.validator.validate_all(df, rules)
        assert result["all_passed"] is False
        assert result["passed"] == 1
        assert result["failed"] == 1
        assert result["total_rules"] == 2


# "A liberdade e o direito de fazer tudo o que as leis permitem." -- Montesquieu
