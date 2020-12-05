"""
Testes para modulo de transformacao
"""

import pytest
import pandas as pd

from etl.transform import DataTransformer


@pytest.fixture
def transformer():
    """Fixture do transformador"""
    return DataTransformer()


@pytest.fixture
def sample_df():
    """DataFrame de exemplo para testes"""
    return pd.DataFrame({
        'id': [1, 2, 3, 2, 4],
        'name': ['Alice', 'Bob', 'Charlie', 'Bob', 'David'],
        'value': [100, 200, None, 200, 400],
        'category': ['A', 'B', 'A', 'B', 'C']
    })


class TestDataTransformer:

    def test_remove_duplicates(self, transformer, sample_df):
        """Testa remocao de duplicatas"""
        result = transformer.remove_duplicates(sample_df, subset=['id', 'name'])

        assert len(result) == 4
        assert 'remove_duplicates' in transformer.transformations_applied

    def test_remove_null_rows(self, transformer, sample_df):
        """Testa remocao de linhas com nulos"""
        result = transformer.remove_null_rows(sample_df, columns=['value'])

        assert len(result) == 4
        assert result['value'].isnull().sum() == 0

    def test_fill_null_values(self, transformer, sample_df):
        """Testa preenchimento de valores nulos"""
        result = transformer.fill_null_values(sample_df, fill_value=0, columns=['value'])

        assert result['value'].isnull().sum() == 0
        assert result.loc[2, 'value'] == 0

    def test_rename_columns(self, transformer, sample_df):
        """Testa renomeacao de colunas"""
        mapping = {'id': 'identifier', 'name': 'full_name'}
        result = transformer.rename_columns(sample_df, mapping)

        assert 'identifier' in result.columns
        assert 'full_name' in result.columns
        assert 'id' not in result.columns

    def test_filter_rows(self, transformer, sample_df):
        """Testa filtragem de linhas"""
        result = transformer.filter_rows(sample_df, lambda df: df['value'] > 150)

        assert len(result) == 2
        assert all(result['value'] > 150)

    def test_select_columns(self, transformer, sample_df):
        """Testa selecao de colunas"""
        result = transformer.select_columns(sample_df, ['id', 'name'])

        assert list(result.columns) == ['id', 'name']
        assert len(result) == len(sample_df)

    def test_add_calculated_column(self, transformer, sample_df):
        """Testa adicao de coluna calculada"""
        result = transformer.add_calculated_column(
            sample_df,
            'double_value',
            lambda df: df['value'] * 2
        )

        assert 'double_value' in result.columns
        assert result.loc[0, 'double_value'] == 200

    def test_apply_transformations(self, transformer, sample_df):
        """Testa aplicacao de multiplas transformacoes"""
        transformations = [
            lambda df: transformer.remove_null_rows(df, columns=['value']),
            lambda df: transformer.filter_rows(df, lambda d: d['value'] > 100)
        ]

        result = transformer.apply_transformations(sample_df, transformations)

        assert len(result) > 0
        assert result['value'].isnull().sum() == 0

    def test_reset_tracking(self, transformer, sample_df):
        """Testa reset do tracking de transformacoes"""
        transformer.remove_duplicates(sample_df)
        assert len(transformer.transformations_applied) > 0

        transformer.reset_tracking()
        assert len(transformer.transformations_applied) == 0
