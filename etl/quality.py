"""
Validacao de qualidade de dados
"""

import pandas as pd
from typing import Any, Callable
from datetime import datetime

from etl.logger import get_logger

logger = get_logger('quality')

class DataQualityRule:
    """Representa uma regra de qualidade de dados"""

    def __init__(self, name: str, check_func: Callable, description: str = ""):
        self.name = name
        self.check_func = check_func
        self.description = description

    def validate(self, df: pd.DataFrame) -> dict[str, Any]:
        """
        Executa validacao

        Returns:
            Dicionario com resultado da validacao
        """
        try:
            result = self.check_func(df)
            return {
                'rule': self.name,
                'passed': result,
                'description': self.description,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Erro ao executar regra {self.name}: {e}")
            return {
                'rule': self.name,
                'passed': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

class DataQualityValidator:
    """Validador de qualidade de dados"""

    def __init__(self):
        self.rules: list[DataQualityRule] = []
        self.results: list[dict[str, Any]] = []

    def add_rule(self, rule: DataQualityRule):
        """Adiciona regra de validacao"""
        self.rules.append(rule)
        return self

    def add_completeness_check(self, columns: list[str], threshold: float = 0.95):
        """
        Adiciona check de completude

        Args:
            columns: Colunas para verificar
            threshold: Threshold minimo de completude (0-1)
        """
        def check(df: pd.DataFrame) -> bool:
            for col in columns:
                if col not in df.columns:
                    logger.error(f"Coluna {col} nao encontrada")
                    return False
                completeness = 1 - (df[col].isna().sum() / len(df))
                if completeness < threshold:
                    logger.warning(f"Coluna {col} abaixo do threshold: {completeness:.2%}")
                    return False
            return True

        rule = DataQualityRule(
            name=f"completeness_{','.join(columns)}",
            check_func=check,
            description=f"Verifica completude >= {threshold:.0%} para {columns}"
        )
        self.add_rule(rule)
        return self

    def add_uniqueness_check(self, columns: list[str]):
        """
        Adiciona check de unicidade

        Args:
            columns: Colunas que devem ser unicas
        """
        def check(df: pd.DataFrame) -> bool:
            duplicates = df[columns].duplicated().sum()
            if duplicates > 0:
                logger.warning(f"Encontradas {duplicates} duplicatas em {columns}")
                return False
            return True

        rule = DataQualityRule(
            name=f"uniqueness_{','.join(columns)}",
            check_func=check,
            description=f"Verifica unicidade de {columns}"
        )
        self.add_rule(rule)
        return self

    def add_range_check(self, column: str, min_value: Any = None, max_value: Any = None):
        """
        Adiciona check de range de valores

        Args:
            column: Coluna para verificar
            min_value: Valor minimo aceitavel
            max_value: Valor maximo aceitavel
        """
        def check(df: pd.DataFrame) -> bool:
            if column not in df.columns:
                return False

            if min_value is not None:
                if (df[column] < min_value).any():
                    logger.warning(f"Valores abaixo de {min_value} em {column}")
                    return False

            if max_value is not None:
                if (df[column] > max_value).any():
                    logger.warning(f"Valores acima de {max_value} em {column}")
                    return False

            return True

        rule = DataQualityRule(
            name=f"range_{column}",
            check_func=check,
            description=f"Verifica range de {column}: [{min_value}, {max_value}]"
        )
        self.add_rule(rule)
        return self

    def add_pattern_check(self, column: str, pattern: str):
        """
        Adiciona check de padrao regex

        Args:
            column: Coluna para verificar
            pattern: Padrao regex
        """
        def check(df: pd.DataFrame) -> bool:
            if column not in df.columns:
                return False

            non_null = df[column].dropna()
            if non_null.empty:
                return True

            matches = non_null.astype(str).str.match(pattern)
            if not matches.all():
                invalid_count = (~matches).sum()
                logger.warning(f"{invalid_count} valores nao correspondem ao padrao em {column}")
                return False

            return True

        rule = DataQualityRule(
            name=f"pattern_{column}",
            check_func=check,
            description=f"Verifica padrao {pattern} em {column}"
        )
        self.add_rule(rule)
        return self

    def add_custom_check(self, name: str, check_func: Callable, description: str = ""):
        """
        Adiciona check customizado

        Args:
            name: Nome da regra
            check_func: Funcao que recebe DataFrame e retorna bool
            description: Descricao da regra
        """
        rule = DataQualityRule(name, check_func, description)
        self.add_rule(rule)
        return self

    def validate(self, df: pd.DataFrame) -> dict[str, Any]:
        """
        Executa todas as validacoes

        Args:
            df: DataFrame para validar

        Returns:
            Relatorio de validacao
        """
        logger.info(f"Executando {len(self.rules)} regras de qualidade")

        self.results = []
        for rule in self.rules:
            result = rule.validate(df)
            self.results.append(result)

        passed_count = sum(1 for r in self.results if r.get('passed', False))
        total_count = len(self.results)

        report = {
            'total_rules': total_count,
            'passed': passed_count,
            'failed': total_count - passed_count,
            'success_rate': passed_count / total_count if total_count > 0 else 0,
            'results': self.results,
            'timestamp': datetime.now().isoformat()
        }

        logger.info(f"Validacao concluida: {passed_count}/{total_count} regras passaram")

        return report

    def get_failed_rules(self) -> list[dict[str, Any]]:
        """Retorna apenas regras que falharam"""
        return [r for r in self.results if not r.get('passed', False)]
