"""
Processadores de dados modulares
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import Callable

class BaseProcessor(ABC):
    """Classe base para processadores de dados"""

    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Processa dados"""
        pass

class CleaningProcessor(BaseProcessor):
    """Processador de limpeza de dados"""

    def __init__(self, remove_duplicates: bool = False, remove_nulls: bool = False):
        self.remove_duplicates = remove_duplicates
        self.remove_nulls = remove_nulls

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Executa limpeza de dados"""
        result = df.copy()

        if self.remove_duplicates:
            result = result.drop_duplicates()

        if self.remove_nulls:
            result = result.dropna()

        return result

class TransformationProcessor(BaseProcessor):
    """Processador de transformacoes customizadas"""

    def __init__(self, transformations: list[Callable] = None):
        self.transformations = transformations or []

    def add_transformation(self, func: Callable):
        """Adiciona transformacao a pipeline"""
        self.transformations.append(func)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Executa transformacoes sequencialmente"""
        result = df.copy()

        for transformation in self.transformations:
            result = transformation(result)

        return result

class ProcessorChain:
    """Encadeia multiplos processadores"""

    def __init__(self):
        self.processors: list[BaseProcessor] = []

    def add(self, processor: BaseProcessor):
        """Adiciona processador a cadeia"""
        self.processors.append(processor)
        return self

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Executa todos os processadores em sequencia"""
        result = df.copy()

        for processor in self.processors:
            result = processor.process(result)

        return result

