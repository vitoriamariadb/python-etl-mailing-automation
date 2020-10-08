"""
Orquestrador de pipeline ETL
"""

import pandas as pd
from typing import List, Dict, Any, Callable, Optional
from datetime import datetime

from .extract import DataExtractor
from .transform import DataTransformer
from .load import DataLoader


class ETLPipeline:
    """Orquestrador de pipeline ETL"""

    def __init__(self, name: str = "ETL Pipeline"):
        self.name = name
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader()
        self.execution_history = []

    def extract_step(self, source: Any, source_type: str = 'csv', **kwargs) -> pd.DataFrame:
        """
        Executa etapa de extracao

        Args:
            source: Fonte de dados
            source_type: Tipo de fonte
            **kwargs: Parametros adicionais

        Returns:
            DataFrame extraido
        """
        return self.extractor.extract(source, source_type, **kwargs)

    def transform_step(self, df: pd.DataFrame, transformations: List[Callable[[pd.DataFrame], pd.DataFrame]]) -> pd.DataFrame:
        """
        Executa etapa de transformacao

        Args:
            df: DataFrame de entrada
            transformations: Lista de transformacoes

        Returns:
            DataFrame transformado
        """
        return self.transformer.apply_transformations(df, transformations)

    def load_step(self, df: pd.DataFrame, destination: Any, format_type: str = 'csv', **kwargs) -> bool:
        """
        Executa etapa de carga

        Args:
            df: DataFrame para carregar
            destination: Destino dos dados
            format_type: Tipo de formato
            **kwargs: Parametros adicionais

        Returns:
            True se sucesso
        """
        return self.loader.load(df, destination, format_type, **kwargs)

    def run(self,
            source: Any,
            destination: Any,
            transformations: Optional[List[Callable[[pd.DataFrame], pd.DataFrame]]] = None,
            source_type: str = 'csv',
            dest_type: str = 'csv',
            extract_params: Optional[Dict[str, Any]] = None,
            load_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Executa pipeline completo

        Args:
            source: Fonte de dados
            destination: Destino dos dados
            transformations: Lista de transformacoes (opcional)
            source_type: Tipo da fonte
            dest_type: Tipo do destino
            extract_params: Parametros para extracao
            load_params: Parametros para carga

        Returns:
            Dicionario com resultado da execucao
        """
        start_time = datetime.now()
        result = {
            'pipeline': self.name,
            'status': 'failed',
            'start_time': start_time,
            'rows_processed': 0,
            'error': None
        }

        try:
            extract_params = extract_params or {}
            load_params = load_params or {}

            df = self.extract_step(source, source_type, **extract_params)
            result['rows_extracted'] = len(df)

            if transformations:
                df = self.transform_step(df, transformations)
                result['transformations_applied'] = len(transformations)

            success = self.load_step(df, destination, dest_type, **load_params)

            if success:
                result['status'] = 'success'
                result['rows_processed'] = len(df)

        except Exception as e:
            result['error'] = str(e)

        finally:
            end_time = datetime.now()
            result['end_time'] = end_time
            result['duration'] = (end_time - start_time).total_seconds()
            self.execution_history.append(result)

        return result

    def get_execution_stats(self) -> Dict[str, Any]:
        """
        Retorna estatisticas de execucao

        Returns:
            Dicionario com estatisticas
        """
        if not self.execution_history:
            return {'total_executions': 0}

        successful = [e for e in self.execution_history if e['status'] == 'success']
        failed = [e for e in self.execution_history if e['status'] == 'failed']

        return {
            'total_executions': len(self.execution_history),
            'successful': len(successful),
            'failed': len(failed),
            'success_rate': len(successful) / len(self.execution_history) * 100,
            'total_rows_processed': sum(e.get('rows_processed', 0) for e in successful)
        }
