"""
Orquestrador de pipeline ETL
"""

import time
from typing import Dict, Any, Optional, List, Callable
import pandas as pd

from etl.connectors import ConnectorFactory
from etl.processors import ProcessorChain, CleaningProcessor, TransformationProcessor
from etl.logger import get_logger


logger = get_logger('orchestrator')


class ETLOrchestrator:
    """Orquestra execucao de pipeline ETL completo"""

    def __init__(self, name: str = "ETL Pipeline"):
        self.name = name
        self.processor_chain = ProcessorChain()

    def add_processor(self, processor):
        """Adiciona processador a pipeline"""
        self.processor_chain.add(processor)
        return self

    def execute(
        self,
        source: str,
        destination: str,
        source_type: str = 'csv',
        dest_type: str = 'csv',
        transformations: Optional[List[Callable]] = None,
        remove_duplicates: bool = False,
        remove_nulls: bool = False
    ) -> Dict[str, Any]:
        """
        Executa pipeline ETL completo

        Args:
            source: Origem dos dados
            destination: Destino dos dados
            source_type: Tipo da fonte
            dest_type: Tipo do destino
            transformations: Lista de funcoes de transformacao
            remove_duplicates: Remove duplicatas
            remove_nulls: Remove nulos

        Returns:
            Dicionario com resultado da execucao
        """
        start_time = time.time()

        try:
            logger.info(f"Iniciando pipeline: {self.name}")

            source_connector = ConnectorFactory.create(source_type)
            logger.info(f"Extraindo dados de {source}")
            df = source_connector.read(source)
            initial_rows = len(df)
            logger.info(f"Extraidos {initial_rows} registros")

            if remove_duplicates or remove_nulls:
                cleaning = CleaningProcessor(
                    remove_duplicates=remove_duplicates,
                    remove_nulls=remove_nulls
                )
                self.processor_chain.add(cleaning)

            if transformations:
                transform = TransformationProcessor(transformations)
                self.processor_chain.add(transform)

            if self.processor_chain.processors:
                logger.info(f"Aplicando {len(self.processor_chain.processors)} processadores")
                df = self.processor_chain.execute(df)
                logger.info(f"Processamento concluido: {len(df)} registros")

            dest_connector = ConnectorFactory.create(dest_type)
            logger.info(f"Carregando dados em {destination}")
            dest_connector.write(df, destination)

            duration = time.time() - start_time

            return {
                'status': 'success',
                'rows_processed': len(df),
                'initial_rows': initial_rows,
                'duration': duration,
                'source': source,
                'destination': destination
            }

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Erro no pipeline: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'duration': duration
            }
