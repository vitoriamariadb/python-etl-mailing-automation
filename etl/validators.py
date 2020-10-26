"""
Validadores de dados para pipeline ETL
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime


class DataValidator:
    """Validador de dados com regras customizaveis"""

    def __init__(self):
        self.validation_results = []

    def validate_not_null(self, df: pd.DataFrame, columns: List[str]) -> Dict[str, Any]:
        """
        Valida que colunas nao possuem valores nulos

        Args:
            df: DataFrame para validar
            columns: Lista de colunas obrigatorias

        Returns:
            Dicionario com resultado da validacao
        """
        result = {
            'rule': 'not_null',
            'passed': True,
            'errors': []
        }

        for col in columns:
            if col not in df.columns:
                result['passed'] = False
                result['errors'].append(f"Coluna {col} nao encontrada")
                continue

            null_count = df[col].isnull().sum()
            if null_count > 0:
                result['passed'] = False
                result['errors'].append(f"Coluna {col} possui {null_count} valores nulos")

        self.validation_results.append(result)
        return result

    def validate_unique(self, df: pd.DataFrame, columns: List[str]) -> Dict[str, Any]:
        """
        Valida que colunas possuem valores unicos

        Args:
            df: DataFrame para validar
            columns: Lista de colunas que devem ser unicas

        Returns:
            Dicionario com resultado da validacao
        """
        result = {
            'rule': 'unique',
            'passed': True,
            'errors': []
        }

        for col in columns:
            if col not in df.columns:
                result['passed'] = False
                result['errors'].append(f"Coluna {col} nao encontrada")
                continue

            duplicates = df[col].duplicated().sum()
            if duplicates > 0:
                result['passed'] = False
                result['errors'].append(f"Coluna {col} possui {duplicates} valores duplicados")

        self.validation_results.append(result)
        return result

    def validate_data_type(self, df: pd.DataFrame, column_types: Dict[str, str]) -> Dict[str, Any]:
        """
        Valida tipos de dados das colunas

        Args:
            df: DataFrame para validar
            column_types: Dicionario com tipos esperados {coluna: tipo}

        Returns:
            Dicionario com resultado da validacao
        """
        result = {
            'rule': 'data_type',
            'passed': True,
            'errors': []
        }

        type_mapping = {
            'int': ['int64', 'int32', 'int16', 'int8'],
            'float': ['float64', 'float32'],
            'string': ['object'],
            'datetime': ['datetime64[ns]'],
            'bool': ['bool']
        }

        for col, expected_type in column_types.items():
            if col not in df.columns:
                result['passed'] = False
                result['errors'].append(f"Coluna {col} nao encontrada")
                continue

            actual_type = str(df[col].dtype)
            expected_types = type_mapping.get(expected_type, [expected_type])

            if actual_type not in expected_types:
                result['passed'] = False
                result['errors'].append(f"Coluna {col} - esperado {expected_type}, obtido {actual_type}")

        self.validation_results.append(result)
        return result

    def validate_range(self, df: pd.DataFrame, column: str, min_value: Optional[Any] = None, max_value: Optional[Any] = None) -> Dict[str, Any]:
        """
        Valida range de valores em uma coluna

        Args:
            df: DataFrame para validar
            column: Nome da coluna
            min_value: Valor minimo permitido
            max_value: Valor maximo permitido

        Returns:
            Dicionario com resultado da validacao
        """
        result = {
            'rule': 'range',
            'passed': True,
            'errors': []
        }

        if column not in df.columns:
            result['passed'] = False
            result['errors'].append(f"Coluna {column} nao encontrada")
            self.validation_results.append(result)
            return result

        if min_value is not None:
            below_min = (df[column] < min_value).sum()
            if below_min > 0:
                result['passed'] = False
                result['errors'].append(f"{below_min} valores abaixo do minimo {min_value}")

        if max_value is not None:
            above_max = (df[column] > max_value).sum()
            if above_max > 0:
                result['passed'] = False
                result['errors'].append(f"{above_max} valores acima do maximo {max_value}")

        self.validation_results.append(result)
        return result

    def validate_custom(self, df: pd.DataFrame, rule_name: str, validation_func: Callable[[pd.DataFrame], bool], error_message: str) -> Dict[str, Any]:
        """
        Valida usando funcao customizada

        Args:
            df: DataFrame para validar
            rule_name: Nome da regra
            validation_func: Funcao que retorna True se valido
            error_message: Mensagem de erro

        Returns:
            Dicionario com resultado da validacao
        """
        result = {
            'rule': rule_name,
            'passed': False,
            'errors': []
        }

        try:
            passed = validation_func(df)
            result['passed'] = passed
            if not passed:
                result['errors'].append(error_message)
        except Exception as e:
            result['errors'].append(f"Erro na validacao: {str(e)}")

        self.validation_results.append(result)
        return result

    def validate_all(self, df: pd.DataFrame, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Executa multiplas validacoes

        Args:
            df: DataFrame para validar
            rules: Lista de regras de validacao

        Returns:
            Dicionario com resultado consolidado
        """
        self.validation_results = []

        for rule in rules:
            rule_type = rule.get('type')

            if rule_type == 'not_null':
                self.validate_not_null(df, rule.get('columns', []))
            elif rule_type == 'unique':
                self.validate_unique(df, rule.get('columns', []))
            elif rule_type == 'data_type':
                self.validate_data_type(df, rule.get('column_types', {}))
            elif rule_type == 'range':
                self.validate_range(df, rule.get('column'), rule.get('min'), rule.get('max'))

        all_passed = all(r['passed'] for r in self.validation_results)

        return {
            'all_passed': all_passed,
            'total_rules': len(self.validation_results),
            'passed': sum(1 for r in self.validation_results if r['passed']),
            'failed': sum(1 for r in self.validation_results if not r['passed']),
            'results': self.validation_results
        }

    def reset(self):
        """Reseta resultados de validacao"""
        self.validation_results = []
