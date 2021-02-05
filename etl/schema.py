"""
Validacao e gestao de schemas de dados
"""

import pandas as pd
from typing import Dict, List, Any, Optional, Union
from enum import Enum

from etl.logger import get_logger


logger = get_logger('schema')


class DataType(Enum):
    """Tipos de dados suportados"""
    STRING = 'string'
    INTEGER = 'integer'
    FLOAT = 'float'
    BOOLEAN = 'boolean'
    DATETIME = 'datetime'
    DATE = 'date'


class ColumnSchema:
    """Definicao de schema de uma coluna"""

    def __init__(
        self,
        name: str,
        dtype: DataType,
        nullable: bool = True,
        unique: bool = False,
        min_value: Optional[Any] = None,
        max_value: Optional[Any] = None
    ):
        self.name = name
        self.dtype = dtype
        self.nullable = nullable
        self.unique = unique
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, series: pd.Series) -> Dict[str, Any]:
        """
        Valida serie contra schema

        Args:
            series: Serie para validar

        Returns:
            Dicionario com resultado da validacao
        """
        errors = []

        if not self.nullable and series.isna().any():
            null_count = series.isna().sum()
            errors.append(f"Coluna nao aceita nulos mas tem {null_count} valores nulos")

        if self.unique and series.duplicated().any():
            dup_count = series.duplicated().sum()
            errors.append(f"Coluna deve ser unica mas tem {dup_count} duplicatas")

        if self.dtype == DataType.INTEGER:
            non_null = series.dropna()
            if not non_null.empty:
                try:
                    pd.to_numeric(non_null, errors='raise')
                except Exception:
                    errors.append("Valores nao numericos encontrados para tipo INTEGER")

        elif self.dtype == DataType.FLOAT:
            non_null = series.dropna()
            if not non_null.empty:
                try:
                    pd.to_numeric(non_null, errors='raise')
                except Exception:
                    errors.append("Valores nao numericos encontrados para tipo FLOAT")

        elif self.dtype == DataType.DATETIME or self.dtype == DataType.DATE:
            non_null = series.dropna()
            if not non_null.empty:
                try:
                    pd.to_datetime(non_null, errors='raise')
                except Exception:
                    errors.append(f"Valores invalidos para tipo {self.dtype.value}")

        if self.min_value is not None:
            non_null = series.dropna()
            if not non_null.empty and (non_null < self.min_value).any():
                errors.append(f"Valores abaixo do minimo {self.min_value}")

        if self.max_value is not None:
            non_null = series.dropna()
            if not non_null.empty and (non_null > self.max_value).any():
                errors.append(f"Valores acima do maximo {self.max_value}")

        return {
            'column': self.name,
            'valid': len(errors) == 0,
            'errors': errors
        }


class TableSchema:
    """Definicao de schema de uma tabela"""

    def __init__(self, name: str):
        self.name = name
        self.columns: Dict[str, ColumnSchema] = {}

    def add_column(self, column: ColumnSchema):
        """Adiciona coluna ao schema"""
        self.columns[column.name] = column
        return self

    def validate(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida DataFrame contra schema

        Args:
            df: DataFrame para validar

        Returns:
            Relatorio de validacao
        """
        logger.info(f"Validando schema {self.name}")

        errors = []
        column_results = []

        df_columns = set(df.columns)
        schema_columns = set(self.columns.keys())

        missing_columns = schema_columns - df_columns
        if missing_columns:
            errors.append(f"Colunas ausentes: {missing_columns}")

        extra_columns = df_columns - schema_columns
        if extra_columns:
            logger.warning(f"Colunas extras nao definidas no schema: {extra_columns}")

        for col_name, col_schema in self.columns.items():
            if col_name in df.columns:
                result = col_schema.validate(df[col_name])
                column_results.append(result)
                if not result['valid']:
                    errors.extend([f"{col_name}: {e}" for e in result['errors']])

        return {
            'schema': self.name,
            'valid': len(errors) == 0,
            'errors': errors,
            'column_results': column_results,
            'missing_columns': list(missing_columns) if missing_columns else [],
            'extra_columns': list(extra_columns) if extra_columns else []
        }

    def to_dict(self) -> Dict[str, Any]:
        """Converte schema para dicionario"""
        return {
            'name': self.name,
            'columns': {
                name: {
                    'dtype': col.dtype.value,
                    'nullable': col.nullable,
                    'unique': col.unique,
                    'min_value': col.min_value,
                    'max_value': col.max_value
                }
                for name, col in self.columns.items()
            }
        }


class SchemaInferrer:
    """Infere schema a partir de DataFrame"""

    @staticmethod
    def infer(df: pd.DataFrame, name: str = "inferred_schema") -> TableSchema:
        """
        Infere schema de DataFrame

        Args:
            df: DataFrame para inferir schema
            name: Nome do schema

        Returns:
            Schema inferido
        """
        schema = TableSchema(name)

        for col in df.columns:
            dtype_map = {
                'object': DataType.STRING,
                'int64': DataType.INTEGER,
                'int32': DataType.INTEGER,
                'float64': DataType.FLOAT,
                'float32': DataType.FLOAT,
                'bool': DataType.BOOLEAN,
                'datetime64[ns]': DataType.DATETIME
            }

            pandas_dtype = str(df[col].dtype)
            data_type = dtype_map.get(pandas_dtype, DataType.STRING)

            nullable = df[col].isna().any()
            unique = df[col].nunique() == len(df)

            column = ColumnSchema(
                name=col,
                dtype=data_type,
                nullable=nullable,
                unique=unique
            )

            schema.add_column(column)

        logger.info(f"Schema inferido com {len(schema.columns)} colunas")
        return schema
