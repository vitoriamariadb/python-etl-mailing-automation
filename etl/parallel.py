"""
Processamento paralelo de dados
"""

import pandas as pd
from typing import Callable, List, Any, Optional
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import numpy as np

from etl.logger import get_logger


logger = get_logger('parallel')


class ParallelProcessor:
    """Processador paralelo de DataFrames"""

    def __init__(self, n_workers: Optional[int] = None, use_processes: bool = True):
        """
        Inicializa processador paralelo

        Args:
            n_workers: Numero de workers (None = CPU count)
            use_processes: True para processos, False para threads
        """
        self.n_workers = n_workers or cpu_count()
        self.use_processes = use_processes
        logger.info(f"Processador paralelo inicializado com {self.n_workers} workers")

    def split_dataframe(self, df: pd.DataFrame, n_chunks: Optional[int] = None) -> List[pd.DataFrame]:
        """
        Divide DataFrame em chunks para processamento paralelo

        Args:
            df: DataFrame para dividir
            n_chunks: Numero de chunks (None = n_workers)

        Returns:
            Lista de DataFrames
        """
        n_chunks = n_chunks or self.n_workers
        chunk_size = len(df) // n_chunks + 1

        chunks = []
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size].copy()
            chunks.append(chunk)

        logger.info(f"DataFrame dividido em {len(chunks)} chunks")
        return chunks

    def process_chunks(
        self,
        df: pd.DataFrame,
        func: Callable[[pd.DataFrame], pd.DataFrame],
        n_chunks: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Processa DataFrame em paralelo aplicando funcao

        Args:
            df: DataFrame para processar
            func: Funcao para aplicar em cada chunk
            n_chunks: Numero de chunks

        Returns:
            DataFrame processado
        """
        chunks = self.split_dataframe(df, n_chunks)

        logger.info(f"Processando {len(chunks)} chunks em paralelo")

        if self.use_processes:
            with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
                results = list(executor.map(func, chunks))
        else:
            with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
                results = list(executor.map(func, chunks))

        result_df = pd.concat(results, ignore_index=True)
        logger.info(f"Processamento paralelo concluido: {len(result_df)} registros")

        return result_df

    def apply_parallel(
        self,
        df: pd.DataFrame,
        func: Callable,
        axis: int = 0
    ) -> pd.Series:
        """
        Aplica funcao em DataFrame paralelamente

        Args:
            df: DataFrame
            func: Funcao para aplicar
            axis: Eixo de aplicacao (0=linhas, 1=colunas)

        Returns:
            Serie com resultados
        """
        if axis == 0:
            chunks = self.split_dataframe(df)

            def apply_func(chunk):
                return chunk.apply(func, axis=axis)

            if self.use_processes:
                with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
                    results = list(executor.map(apply_func, chunks))
            else:
                with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
                    results = list(executor.map(apply_func, chunks))

            return pd.concat(results, ignore_index=True)
        else:
            return df.apply(func, axis=axis)


class BatchProcessor:
    """Processa dados em batches"""

    def __init__(self, batch_size: int = 1000):
        """
        Inicializa processador de batches

        Args:
            batch_size: Tamanho de cada batch
        """
        self.batch_size = batch_size

    def process_in_batches(
        self,
        df: pd.DataFrame,
        func: Callable[[pd.DataFrame], Any],
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> List[Any]:
        """
        Processa DataFrame em batches

        Args:
            df: DataFrame para processar
            func: Funcao para aplicar em cada batch
            progress_callback: Callback de progresso (current, total)

        Returns:
            Lista de resultados
        """
        total_batches = len(df) // self.batch_size + (1 if len(df) % self.batch_size else 0)
        logger.info(f"Processando {len(df)} registros em {total_batches} batches")

        results = []
        for i, start in enumerate(range(0, len(df), self.batch_size)):
            end = min(start + self.batch_size, len(df))
            batch = df.iloc[start:end]

            result = func(batch)
            results.append(result)

            if progress_callback:
                progress_callback(i + 1, total_batches)

            if (i + 1) % 10 == 0:
                logger.info(f"Processados {i + 1}/{total_batches} batches")

        logger.info(f"Processamento de batches concluido")
        return results


class AsyncProcessor:
    """Processador assincrono de tarefas"""

    def __init__(self, max_workers: Optional[int] = None):
        """
        Inicializa processador assincrono

        Args:
            max_workers: Numero maximo de workers
        """
        self.max_workers = max_workers or cpu_count()

    def process_tasks(
        self,
        tasks: List[Callable],
        use_processes: bool = False
    ) -> List[Any]:
        """
        Processa lista de tarefas assincronamente

        Args:
            tasks: Lista de funcoes/callables
            use_processes: True para processos, False para threads

        Returns:
            Lista de resultados
        """
        logger.info(f"Processando {len(tasks)} tarefas assincronamente")

        Executor = ProcessPoolExecutor if use_processes else ThreadPoolExecutor

        results = []
        with Executor(max_workers=self.max_workers) as executor:
            future_to_task = {executor.submit(task): i for i, task in enumerate(tasks)}

            for future in as_completed(future_to_task):
                task_idx = future_to_task[future]
                try:
                    result = future.result()
                    results.append((task_idx, result))
                except Exception as e:
                    logger.error(f"Tarefa {task_idx} falhou: {e}")
                    results.append((task_idx, None))

        results.sort(key=lambda x: x[0])
        return [r[1] for r in results]
