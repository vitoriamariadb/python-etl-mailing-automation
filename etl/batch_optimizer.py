"""
Otimizacoes para processamento em batch
"""

import pandas as pd
import numpy as np
from typing import Callable, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from etl.logger import get_logger

logger = get_logger('batch_optimizer')

class AdaptiveBatchProcessor:
    """Processador de batch com tamanho adaptativo"""

    def __init__(
        self,
        initial_batch_size: int = 1000,
        min_batch_size: int = 100,
        max_batch_size: int = 10000,
        target_time: float = 1.0
    ):
        """
        Inicializa processador adaptativo

        Args:
            initial_batch_size: Tamanho inicial de batch
            min_batch_size: Tamanho minimo
            max_batch_size: Tamanho maximo
            target_time: Tempo alvo por batch (segundos)
        """
        self.batch_size = initial_batch_size
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.target_time = target_time
        self.batch_times: list[float] = []

    def _adjust_batch_size(self, execution_time: float):
        """Ajusta tamanho de batch baseado no tempo de execucao"""
        if execution_time > self.target_time * 1.2:
            new_size = int(self.batch_size * 0.8)
            self.batch_size = max(new_size, self.min_batch_size)
            logger.info(f"Reduzindo batch size para {self.batch_size}")

        elif execution_time < self.target_time * 0.8:
            new_size = int(self.batch_size * 1.2)
            self.batch_size = min(new_size, self.max_batch_size)
            logger.info(f"Aumentando batch size para {self.batch_size}")

        self.batch_times.append(execution_time)

    def process(
        self,
        df: pd.DataFrame,
        func: Callable[[pd.DataFrame], Any],
        progress_callback: Callable[[int, int | None, None]] = None
    ) -> list[Any]:
        """
        Processa DataFrame com batch size adaptativo

        Args:
            df: DataFrame para processar
            func: Funcao de processamento
            progress_callback: Callback de progresso

        Returns:
            Lista de resultados
        """
        results = []
        start_idx = 0
        total_rows = len(df)
        batch_count = 0

        while start_idx < total_rows:
            end_idx = min(start_idx + self.batch_size, total_rows)
            batch = df.iloc[start_idx:end_idx]

            start_time = time.time()
            result = func(batch)
            execution_time = time.time() - start_time

            results.append(result)
            batch_count += 1

            self._adjust_batch_size(execution_time)

            if progress_callback:
                progress_callback(end_idx, total_rows)

            logger.debug(f"Batch {batch_count}: {len(batch)} linhas em {execution_time:.2f}s")

            start_idx = end_idx

        avg_time = np.mean(self.batch_times) if self.batch_times else 0
        logger.info(f"Processados {batch_count} batches - tempo medio: {avg_time:.2f}s")

        return results

class MemoryEfficientBatchProcessor:
    """Processador otimizado para uso de memoria"""

    def __init__(self, max_memory_mb: int = 512):
        """
        Inicializa processador eficiente em memoria

        Args:
            max_memory_mb: Memoria maxima em MB
        """
        self.max_memory_mb = max_memory_mb

    def _estimate_batch_size(self, df: pd.DataFrame) -> int:
        """Estima tamanho de batch baseado em memoria disponivel"""
        sample_size = min(1000, len(df))
        sample = df.head(sample_size)

        memory_per_row = sample.memory_usage(deep=True).sum() / sample_size
        max_memory_bytes = self.max_memory_mb * 1024 * 1024

        batch_size = int(max_memory_bytes / memory_per_row * 0.8)
        batch_size = max(100, min(batch_size, len(df)))

        logger.info(f"Tamanho de batch calculado: {batch_size} linhas")
        return batch_size

    def process_chunked(
        self,
        df: pd.DataFrame,
        func: Callable[[pd.DataFrame], pd.DataFrame]
    ) -> pd.DataFrame:
        """
        Processa DataFrame em chunks otimizados para memoria

        Args:
            df: DataFrame para processar
            func: Funcao de processamento

        Returns:
            DataFrame processado
        """
        batch_size = self._estimate_batch_size(df)
        results = []

        for start in range(0, len(df), batch_size):
            end = min(start + batch_size, len(df))
            chunk = df.iloc[start:end].copy()

            processed = func(chunk)
            results.append(processed)

            del chunk

            if (start // batch_size) % 10 == 0:
                logger.info(f"Processadas {end}/{len(df)} linhas")

        return pd.concat(results, ignore_index=True)

class ParallelBatchProcessor:
    """Processador de batches com paralelizacao otimizada"""

    def __init__(self, n_workers: int = 4, batch_size: int = 1000):
        """
        Inicializa processador paralelo de batches

        Args:
            n_workers: Numero de workers
            batch_size: Tamanho de cada batch
        """
        self.n_workers = n_workers
        self.batch_size = batch_size

    def process_parallel(
        self,
        df: pd.DataFrame,
        func: Callable[[pd.DataFrame], Any]
    ) -> list[Any]:
        """
        Processa batches em paralelo

        Args:
            df: DataFrame para processar
            func: Funcao de processamento

        Returns:
            Lista de resultados
        """
        batches = []
        for start in range(0, len(df), self.batch_size):
            end = min(start + self.batch_size, len(df))
            batches.append(df.iloc[start:end].copy())

        logger.info(f"Processando {len(batches)} batches em {self.n_workers} workers")

        results = []
        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            future_to_batch = {executor.submit(func, batch): i for i, batch in enumerate(batches)}

            for future in as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    result = future.result()
                    results.append((batch_idx, result))
                except Exception as e:
                    logger.error(f"Erro no batch {batch_idx}: {e}")
                    results.append((batch_idx, None))

        results.sort(key=lambda x: x[0])
        return [r[1] for r in results]

class StreamingBatchProcessor:
    """Processador de batches em modo streaming"""

    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size

    def process_stream(
        self,
        file_path: str,
        func: Callable[[pd.DataFrame], Any],
        chunksize: int | None = None
    ) -> list[Any]:
        """
        Processa arquivo em modo streaming

        Args:
            file_path: Caminho do arquivo
            func: Funcao de processamento
            chunksize: Tamanho de cada chunk

        Returns:
            Lista de resultados
        """
        chunksize = chunksize or self.batch_size
        results = []

        logger.info(f"Processando arquivo em streaming: {file_path}")

        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunksize)):
            result = func(chunk)
            results.append(result)

            if (i + 1) % 10 == 0:
                logger.info(f"Processados {(i + 1) * chunksize} registros")

        logger.info(f"Streaming concluido: {len(results)} chunks processados")
        return results

