"""
Modulo de transformacao de dados
"""

import pandas as pd
from typing import List, Callable, Optional, Dict, Any


class DataTransformer:
    """Transformador de dados com operacoes comuns"""

    def __init__(self):
        self.transformations_applied = []

    def remove_duplicates(self, df: pd.DataFrame, subset: Optional[List[str]] = None, keep: str = 'first') -> pd.DataFrame:
        """
        Remove linhas duplicadas

        Args:
            df: DataFrame de entrada
            subset: Colunas para considerar na deteccao de duplicatas
            keep: Qual duplicata manter ('first', 'last', False)

        Returns:
            DataFrame sem duplicatas
        """
        result = df.drop_duplicates(subset=subset, keep=keep)
        self.transformations_applied.append('remove_duplicates')
        return result

    def remove_null_rows(self, df: pd.DataFrame, columns: Optional[List[str]] = None, how: str = 'any') -> pd.DataFrame:
        """
        Remove linhas com valores nulos

        Args:
            df: DataFrame de entrada
            columns: Colunas para verificar nulos
            how: 'any' ou 'all'

        Returns:
            DataFrame sem nulos
        """
        result = df.dropna(subset=columns, how=how)
        self.transformations_applied.append('remove_null_rows')
        return result

    def fill_null_values(self, df: pd.DataFrame, fill_value: Any = 0, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Preenche valores nulos

        Args:
            df: DataFrame de entrada
            fill_value: Valor para preencher nulos
            columns: Colunas para preencher

        Returns:
            DataFrame com nulos preenchidos
        """
        if columns:
            result = df.copy()
            result[columns] = result[columns].fillna(fill_value)
        else:
            result = df.fillna(fill_value)

        self.transformations_applied.append('fill_null_values')
        return result

    def rename_columns(self, df: pd.DataFrame, column_mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Renomeia colunas

        Args:
            df: DataFrame de entrada
            column_mapping: Dicionario com mapeamento antigo -> novo

        Returns:
            DataFrame com colunas renomeadas
        """
        result = df.rename(columns=column_mapping)
        self.transformations_applied.append('rename_columns')
        return result

    def filter_rows(self, df: pd.DataFrame, condition: Callable[[pd.DataFrame], pd.Series]) -> pd.DataFrame:
        """
        Filtra linhas baseado em condicao

        Args:
            df: DataFrame de entrada
            condition: Funcao que retorna Series booleana

        Returns:
            DataFrame filtrado
        """
        result = df[condition(df)]
        self.transformations_applied.append('filter_rows')
        return result

    def select_columns(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Seleciona colunas especificas

        Args:
            df: DataFrame de entrada
            columns: Lista de colunas para selecionar

        Returns:
            DataFrame com colunas selecionadas
        """
        result = df[columns]
        self.transformations_applied.append('select_columns')
        return result

    def add_calculated_column(self, df: pd.DataFrame, column_name: str, calculation: Callable[[pd.DataFrame], pd.Series]) -> pd.DataFrame:
        """
        Adiciona coluna calculada

        Args:
            df: DataFrame de entrada
            column_name: Nome da nova coluna
            calculation: Funcao que calcula os valores

        Returns:
            DataFrame com nova coluna
        """
        result = df.copy()
        result[column_name] = calculation(df)
        self.transformations_applied.append('add_calculated_column')
        return result

    def apply_transformations(self, df: pd.DataFrame, transformations: List[Callable[[pd.DataFrame], pd.DataFrame]]) -> pd.DataFrame:
        """
        Aplica lista de transformacoes em sequencia

        Args:
            df: DataFrame de entrada
            transformations: Lista de funcoes de transformacao

        Returns:
            DataFrame transformado
        """
        result = df.copy()
        for transform in transformations:
            result = transform(result)
        return result

    def reset_tracking(self):
        """Reset do tracking de transformacoes aplicadas"""
        self.transformations_applied = []
