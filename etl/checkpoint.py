"""
Sistema de checkpoint e recuperacao de pipeline
"""

import json
import pickle
from pathlib import Path
from typing import Dict, Any, Optional, Union
from datetime import datetime
import hashlib

from etl.logger import get_logger


logger = get_logger('checkpoint')


class CheckpointManager:
    """Gerencia checkpoints de execucao de pipeline"""

    def __init__(self, checkpoint_dir: Union[str, Path] = 'checkpoints'):
        """
        Inicializa gerenciador de checkpoints

        Args:
            checkpoint_dir: Diretorio para armazenar checkpoints
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.current_checkpoint: Optional[str] = None

    def _generate_checkpoint_id(self, pipeline_name: str) -> str:
        """Gera ID unico para checkpoint"""
        timestamp = datetime.now().isoformat()
        content = f"{pipeline_name}_{timestamp}"
        return hashlib.md5(content.encode()).hexdigest()[:16]

    def create_checkpoint(
        self,
        pipeline_name: str,
        step: str,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Cria checkpoint de execucao

        Args:
            pipeline_name: Nome do pipeline
            step: Nome do passo atual
            data: Dados para salvar
            metadata: Metadados adicionais

        Returns:
            ID do checkpoint criado
        """
        checkpoint_id = self._generate_checkpoint_id(pipeline_name)

        checkpoint_data = {
            'checkpoint_id': checkpoint_id,
            'pipeline_name': pipeline_name,
            'step': step,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }

        metadata_file = self.checkpoint_dir / f"{checkpoint_id}_meta.json"
        with open(metadata_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)

        data_file = self.checkpoint_dir / f"{checkpoint_id}_data.pkl"
        with open(data_file, 'wb') as f:
            pickle.dump(data, f)

        self.current_checkpoint = checkpoint_id
        logger.info(f"Checkpoint criado: {checkpoint_id} (step: {step})")

        return checkpoint_id

    def load_checkpoint(self, checkpoint_id: str) -> Dict[str, Any]:
        """
        Carrega checkpoint

        Args:
            checkpoint_id: ID do checkpoint

        Returns:
            Dicionario com metadados e dados
        """
        metadata_file = self.checkpoint_dir / f"{checkpoint_id}_meta.json"
        data_file = self.checkpoint_dir / f"{checkpoint_id}_data.pkl"

        if not metadata_file.exists() or not data_file.exists():
            raise FileNotFoundError(f"Checkpoint nao encontrado: {checkpoint_id}")

        with open(metadata_file, 'r') as f:
            metadata = json.load(f)

        with open(data_file, 'rb') as f:
            data = pickle.load(f)

        logger.info(f"Checkpoint carregado: {checkpoint_id}")

        return {
            'metadata': metadata,
            'data': data
        }

    def list_checkpoints(self, pipeline_name: Optional[str] = None) -> list:
        """
        Lista checkpoints disponiveis

        Args:
            pipeline_name: Filtrar por nome de pipeline

        Returns:
            Lista de checkpoints
        """
        checkpoints = []

        for meta_file in self.checkpoint_dir.glob('*_meta.json'):
            with open(meta_file, 'r') as f:
                metadata = json.load(f)

            if pipeline_name is None or metadata['pipeline_name'] == pipeline_name:
                checkpoints.append(metadata)

        checkpoints.sort(key=lambda x: x['timestamp'], reverse=True)
        return checkpoints

    def get_latest_checkpoint(self, pipeline_name: str) -> Optional[Dict[str, Any]]:
        """
        Recupera checkpoint mais recente de um pipeline

        Args:
            pipeline_name: Nome do pipeline

        Returns:
            Checkpoint ou None
        """
        checkpoints = self.list_checkpoints(pipeline_name)

        if not checkpoints:
            return None

        latest = checkpoints[0]
        return self.load_checkpoint(latest['checkpoint_id'])

    def delete_checkpoint(self, checkpoint_id: str):
        """
        Deleta checkpoint

        Args:
            checkpoint_id: ID do checkpoint
        """
        metadata_file = self.checkpoint_dir / f"{checkpoint_id}_meta.json"
        data_file = self.checkpoint_dir / f"{checkpoint_id}_data.pkl"

        if metadata_file.exists():
            metadata_file.unlink()

        if data_file.exists():
            data_file.unlink()

        logger.info(f"Checkpoint deletado: {checkpoint_id}")

    def cleanup_old_checkpoints(self, pipeline_name: str, keep_last: int = 5):
        """
        Remove checkpoints antigos

        Args:
            pipeline_name: Nome do pipeline
            keep_last: Numero de checkpoints recentes para manter
        """
        checkpoints = self.list_checkpoints(pipeline_name)

        if len(checkpoints) <= keep_last:
            return

        to_delete = checkpoints[keep_last:]
        for checkpoint in to_delete:
            self.delete_checkpoint(checkpoint['checkpoint_id'])

        logger.info(f"Removidos {len(to_delete)} checkpoints antigos")


class RecoverableStep:
    """Decorador para tornar passo de pipeline recuperavel"""

    def __init__(self, checkpoint_manager: CheckpointManager, step_name: str):
        self.checkpoint_manager = checkpoint_manager
        self.step_name = step_name

    def __call__(self, func):
        """Wrapper para funcao de passo"""
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)

                self.checkpoint_manager.create_checkpoint(
                    pipeline_name=kwargs.get('pipeline_name', 'default'),
                    step=self.step_name,
                    data=result,
                    metadata={'args': str(args), 'kwargs': str(kwargs)}
                )

                return result

            except Exception as e:
                logger.error(f"Erro no passo {self.step_name}: {e}")
                raise

        return wrapper


class PipelineRecovery:
    """Gerencia recuperacao de pipeline a partir de checkpoint"""

    def __init__(self, checkpoint_manager: CheckpointManager):
        self.checkpoint_manager = checkpoint_manager

    def can_recover(self, pipeline_name: str) -> bool:
        """
        Verifica se pipeline pode ser recuperado

        Args:
            pipeline_name: Nome do pipeline

        Returns:
            True se existe checkpoint
        """
        latest = self.checkpoint_manager.get_latest_checkpoint(pipeline_name)
        return latest is not None

    def recover(self, pipeline_name: str) -> Dict[str, Any]:
        """
        Recupera estado de pipeline

        Args:
            pipeline_name: Nome do pipeline

        Returns:
            Estado recuperado
        """
        checkpoint = self.checkpoint_manager.get_latest_checkpoint(pipeline_name)

        if checkpoint is None:
            raise ValueError(f"Nenhum checkpoint encontrado para: {pipeline_name}")

        logger.info(f"Recuperando pipeline {pipeline_name} do passo: {checkpoint['metadata']['step']}")

        return checkpoint

    def resume_from_step(
        self,
        pipeline_name: str,
        remaining_steps: list
    ) -> Any:
        """
        Resume pipeline a partir de checkpoint

        Args:
            pipeline_name: Nome do pipeline
            remaining_steps: Passos restantes para executar

        Returns:
            Resultado final
        """
        checkpoint = self.recover(pipeline_name)
        data = checkpoint['data']

        logger.info(f"Executando {len(remaining_steps)} passos restantes")

        for step in remaining_steps:
            data = step(data)

        return data
