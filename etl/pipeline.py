"""
Pipeline ETL principal com documentacao completa

Este modulo fornece a classe principal ETLPipeline que orquestra
a execucao de pipelines ETL (Extract, Transform, Load).
"""

import time
from typing import Optional, List, Callable, Dict, Any
import pandas as pd

from etl.extract import DataExtractor
from etl.transform import DataTransformer
from etl.load import DataLoader
from etl.logger import get_logger
from etl.exceptions import ETLException


logger = get_logger('pipeline')


class ETLPipeline:
    """
    Pipeline ETL para extracao, transformacao e carga de dados

    Esta classe orquestra todo o processo ETL, incluindo:
    - Extracao de dados de multiplas fontes
    - Aplicacao de transformacoes
    - Validacao de dados
    - Carga em destinos variados
    - Metricas e monitoramento

    Attributes:
        name: Nome do pipeline
        extractor: Instancia de DataExtractor
        transformer: Instancia de DataTransformer
        loader: Instancia de DataLoader
        execution_history: Lista de execucoes anteriores

    Example:
        >>> pipeline = ETLPipeline(name="Sales Pipeline")
        >>> result = pipeline.run(
        ...     source='data/sales.csv',
        ...     destination='output/processed.csv',
        ...     source_type='csv',
        ...     dest_type='csv'
        ... )
        >>> print(result['status'])
        'success'
    """

    def __init__(self, name: str = "ETL Pipeline"):
        """
        Inicializa pipeline ETL

        Args:
            name: Nome identificador do pipeline

        Returns:
            None
        """
        self.name = name
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader()
        self.execution_history: List[Dict[str, Any]] = []
        logger.info(f"Pipeline criado: {name}")

    def run(
        self,
        source: str,
        destination: str,
        transformations: Optional[List[Callable]] = None,
        source_type: str = 'csv',
        dest_type: str = 'csv',
        **kwargs
    ) -> Dict[str, Any]:
        """
        Executa pipeline ETL completo

        Este metodo executa todas as etapas do pipeline:
        1. Extrai dados da fonte
        2. Aplica transformacoes (se fornecidas)
        3. Carrega dados no destino
        4. Registra metricas de execucao

        Args:
            source: Caminho ou URL da fonte de dados
            destination: Caminho do arquivo de destino
            transformations: Lista de funcoes de transformacao
            source_type: Tipo da fonte (csv, json, excel)
            dest_type: Tipo do destino (csv, json, excel, parquet)
            **kwargs: Parametros adicionais para extrator/loader

        Returns:
            Dict contendo:
                - status: 'success' ou 'failed'
                - rows_processed: Numero de registros processados
                - duration: Tempo de execucao em segundos
                - source: Fonte dos dados
                - destination: Destino dos dados
                - error: Mensagem de erro (se falhou)

        Raises:
            ETLException: Se ocorrer erro durante execucao

        Example:
            >>> pipeline = ETLPipeline("Example")
            >>> result = pipeline.run(
            ...     source='input.csv',
            ...     destination='output.parquet',
            ...     source_type='csv',
            ...     dest_type='parquet'
            ... )
        """
        start_time = time.time()

        try:
            logger.info(f"Iniciando pipeline: {self.name}")
            logger.info(f"Fonte: {source} ({source_type})")
            logger.info(f"Destino: {destination} ({dest_type})")

            df = self.extractor.extract(source, source_type, **kwargs)
            logger.info(f"Extraidos {len(df)} registros")

            if transformations:
                logger.info(f"Aplicando {len(transformations)} transformacoes")
                for i, transform_func in enumerate(transformations, 1):
                    df = transform_func(df)
                    logger.info(f"Transformacao {i} concluida: {len(df)} registros")

            self.loader.load(df, destination, dest_type, **kwargs)
            logger.info(f"Dados carregados em {destination}")

            duration = time.time() - start_time

            result = {
                'status': 'success',
                'rows_processed': len(df),
                'duration': duration,
                'source': source,
                'destination': destination
            }

            self.execution_history.append(result)
            logger.info(f"Pipeline concluido em {duration:.2f}s")

            return result

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Erro no pipeline: {e}")

            result = {
                'status': 'failed',
                'error': str(e),
                'duration': duration,
                'source': source,
                'destination': destination
            }

            self.execution_history.append(result)
            return result

    def get_execution_stats(self) -> Dict[str, Any]:
        """
        Retorna estatisticas de execucao do pipeline

        Calcula metricas agregadas baseadas no historico de execucoes,
        incluindo taxas de sucesso, tempo medio, total de registros, etc.

        Returns:
            Dict contendo:
                - total_executions: Numero total de execucoes
                - successful_executions: Numero de execucoes bem-sucedidas
                - failed_executions: Numero de execucoes falhadas
                - success_rate: Taxa de sucesso (0-1)
                - avg_duration: Tempo medio de execucao
                - total_rows_processed: Total de registros processados

        Example:
            >>> pipeline = ETLPipeline("Example")
            >>> pipeline.run(...)
            >>> stats = pipeline.get_execution_stats()
            >>> print(f"Taxa de sucesso: {stats['success_rate']:.1%}")
        """
        if not self.execution_history:
            return {
                'total_executions': 0,
                'successful_executions': 0,
                'failed_executions': 0,
                'success_rate': 0.0,
                'avg_duration': 0.0,
                'total_rows_processed': 0
            }

        successful = [e for e in self.execution_history if e['status'] == 'success']
        failed = [e for e in self.execution_history if e['status'] == 'failed']

        total_rows = sum(e.get('rows_processed', 0) for e in successful)
        avg_duration = sum(e['duration'] for e in self.execution_history) / len(self.execution_history)

        return {
            'total_executions': len(self.execution_history),
            'successful_executions': len(successful),
            'failed_executions': len(failed),
            'success_rate': len(successful) / len(self.execution_history),
            'avg_duration': avg_duration,
            'total_rows_processed': total_rows
        }
