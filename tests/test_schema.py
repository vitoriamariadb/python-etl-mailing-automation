"""
Testes para modulo de validacao de schema
"""

import pytest
import pandas as pd

from etl.schema import DataType, ColumnSchema, TableSchema, SchemaInferrer


class TestSchema:
    """Testes para validacao de schema"""

    @pytest.fixture
    def sample_data(self):
        """Dados de exemplo"""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'active': [True, False, True]
        })

    def test_column_schema_validation_pass(self):
        """Testa validacao de coluna que passa"""
        col = ColumnSchema('id', DataType.INTEGER, nullable=False, unique=True)
        series = pd.Series([1, 2, 3], name='id')

        result = col.validate(series)

        assert result['valid'] is True
        assert len(result['errors']) == 0

    def test_column_schema_nullable_fail(self):
        """Testa validacao de nullable que falha"""
        col = ColumnSchema('name', DataType.STRING, nullable=False)
        series = pd.Series(['Alice', None, 'Bob'], name='name')

        result = col.validate(series)

        assert result['valid'] is False
        assert len(result['errors']) > 0

    def test_column_schema_unique_fail(self):
        """Testa validacao de unicidade que falha"""
        col = ColumnSchema('id', DataType.INTEGER, unique=True)
        series = pd.Series([1, 2, 2], name='id')

        result = col.validate(series)

        assert result['valid'] is False

    def test_column_schema_range_validation(self):
        """Testa validacao de range"""
        col = ColumnSchema('age', DataType.INTEGER, min_value=0, max_value=100)
        series = pd.Series([25, 30, 35], name='age')

        result = col.validate(series)

        assert result['valid'] is True

    def test_column_schema_range_fail(self):
        """Testa validacao de range que falha"""
        col = ColumnSchema('age', DataType.INTEGER, min_value=0, max_value=50)
        series = pd.Series([25, 30, 75], name='age')

        result = col.validate(series)

        assert result['valid'] is False

    def test_table_schema_validation_pass(self, sample_data):
        """Testa validacao de tabela que passa"""
        schema = TableSchema('users')
        schema.add_column(ColumnSchema('id', DataType.INTEGER, nullable=False, unique=True))
        schema.add_column(ColumnSchema('name', DataType.STRING, nullable=False))
        schema.add_column(ColumnSchema('age', DataType.INTEGER))
        schema.add_column(ColumnSchema('active', DataType.BOOLEAN))

        result = schema.validate(sample_data)

        assert result['valid'] is True

    def test_table_schema_missing_columns(self):
        """Testa schema com colunas faltando"""
        df = pd.DataFrame({'id': [1, 2]})

        schema = TableSchema('test')
        schema.add_column(ColumnSchema('id', DataType.INTEGER))
        schema.add_column(ColumnSchema('name', DataType.STRING))

        result = schema.validate(df)

        assert result['valid'] is False
        assert 'name' in result['missing_columns']

    def test_table_schema_extra_columns(self, sample_data):
        """Testa schema com colunas extras"""
        schema = TableSchema('test')
        schema.add_column(ColumnSchema('id', DataType.INTEGER))
        schema.add_column(ColumnSchema('name', DataType.STRING))

        result = schema.validate(sample_data)

        assert 'age' in result['extra_columns']
        assert 'active' in result['extra_columns']

    def test_schema_to_dict(self):
        """Testa conversao de schema para dicionario"""
        schema = TableSchema('test')
        schema.add_column(ColumnSchema('id', DataType.INTEGER, nullable=False))

        schema_dict = schema.to_dict()

        assert schema_dict['name'] == 'test'
        assert 'id' in schema_dict['columns']
        assert schema_dict['columns']['id']['dtype'] == 'integer'

    def test_schema_inferrer(self, sample_data):
        """Testa inferencia de schema"""
        schema = SchemaInferrer.infer(sample_data, 'inferred')

        assert schema.name == 'inferred'
        assert len(schema.columns) == 4
        assert 'id' in schema.columns
        assert 'name' in schema.columns

    def test_schema_inferrer_data_types(self):
        """Testa inferencia de tipos de dados"""
        df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.5, 2.5, 3.5],
            'str_col': ['a', 'b', 'c'],
            'bool_col': [True, False, True]
        })

        schema = SchemaInferrer.infer(df)

        assert schema.columns['int_col'].dtype == DataType.INTEGER
        assert schema.columns['float_col'].dtype == DataType.FLOAT
        assert schema.columns['str_col'].dtype == DataType.STRING
        assert schema.columns['bool_col'].dtype == DataType.BOOLEAN

    def test_schema_inferrer_nullable(self):
        """Testa inferencia de nullable"""
        df = pd.DataFrame({
            'col1': [1, 2, None],
            'col2': [1, 2, 3]
        })

        schema = SchemaInferrer.infer(df)

        assert schema.columns['col1'].nullable is True
        assert schema.columns['col2'].nullable is False

