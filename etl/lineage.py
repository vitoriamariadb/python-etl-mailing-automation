"""
Rastreamento de linhagem de dados (data lineage)
"""

import json
from typing import Any
from datetime import datetime
from pathlib import Path
import hashlib

from etl.logger import get_logger

logger = get_logger('lineage')

class DataLineageNode:
    """Representa um no na linhagem de dados"""

    def __init__(
        self,
        node_id: str,
        node_type: str,
        name: str,
        metadata: dict[str, Any | None] = None
    ):
        self.node_id = node_id
        self.node_type = node_type
        self.name = name
        self.metadata = metadata or {}
        self.timestamp = datetime.now().isoformat()

    def to_dict(self) -> dict[str, Any]:
        """Converte no para dicionario"""
        return {
            'node_id': self.node_id,
            'node_type': self.node_type,
            'name': self.name,
            'metadata': self.metadata,
            'timestamp': self.timestamp
        }

class DataLineageEdge:
    """Representa uma aresta (relacionamento) na linhagem"""

    def __init__(
        self,
        source_id: str,
        target_id: str,
        edge_type: str,
        metadata: dict[str, Any | None] = None
    ):
        self.source_id = source_id
        self.target_id = target_id
        self.edge_type = edge_type
        self.metadata = metadata or {}
        self.timestamp = datetime.now().isoformat()

    def to_dict(self) -> dict[str, Any]:
        """Converte aresta para dicionario"""
        return {
            'source_id': self.source_id,
            'target_id': self.target_id,
            'edge_type': self.edge_type,
            'metadata': self.metadata,
            'timestamp': self.timestamp
        }

class DataLineageTracker:
    """Rastreia linhagem de dados em pipeline ETL"""

    def __init__(self, output_file: Path | None = None):
        """
        Inicializa tracker de linhagem

        Args:
            output_file: Arquivo para salvar linhagem
        """
        self.nodes: dict[str, DataLineageNode] = {}
        self.edges: list[DataLineageEdge] = []
        self.output_file = output_file or Path('lineage/data_lineage.json')
        self.pipeline_id = self._generate_pipeline_id()

    def _generate_pipeline_id(self) -> str:
        """Gera ID unico para pipeline"""
        timestamp = datetime.now().isoformat()
        return hashlib.md5(timestamp.encode()).hexdigest()[:12]

    def add_source(self, source_id: str, name: str, metadata: dict | None = None):
        """
        Adiciona fonte de dados

        Args:
            source_id: ID da fonte
            name: Nome da fonte
            metadata: Metadados adicionais
        """
        node = DataLineageNode(
            node_id=source_id,
            node_type='source',
            name=name,
            metadata=metadata
        )
        self.nodes[source_id] = node
        logger.info(f"Fonte adicionada: {name}")

    def add_transformation(self, transform_id: str, name: str, metadata: dict | None = None):
        """
        Adiciona transformacao

        Args:
            transform_id: ID da transformacao
            name: Nome da transformacao
            metadata: Metadados adicionais
        """
        node = DataLineageNode(
            node_id=transform_id,
            node_type='transformation',
            name=name,
            metadata=metadata
        )
        self.nodes[transform_id] = node
        logger.info(f"Transformacao adicionada: {name}")

    def add_target(self, target_id: str, name: str, metadata: dict | None = None):
        """
        Adiciona destino de dados

        Args:
            target_id: ID do destino
            name: Nome do destino
            metadata: Metadados adicionais
        """
        node = DataLineageNode(
            node_id=target_id,
            node_type='target',
            name=name,
            metadata=metadata
        )
        self.nodes[target_id] = node
        logger.info(f"Destino adicionado: {name}")

    def add_relationship(
        self,
        source_id: str,
        target_id: str,
        edge_type: str = 'flows_to',
        metadata: dict | None = None
    ):
        """
        Adiciona relacionamento entre nos

        Args:
            source_id: ID do no origem
            target_id: ID do no destino
            edge_type: Tipo de relacionamento
            metadata: Metadados adicionais
        """
        edge = DataLineageEdge(
            source_id=source_id,
            target_id=target_id,
            edge_type=edge_type,
            metadata=metadata
        )
        self.edges.append(edge)
        logger.info(f"Relacionamento adicionado: {source_id} -> {target_id}")

    def track_pipeline_execution(
        self,
        source: str,
        transformations: list[str],
        target: str,
        metadata: dict | None = None
    ):
        """
        Rastreia execucao completa de pipeline

        Args:
            source: Fonte de dados
            transformations: Lista de transformacoes
            target: Destino de dados
            metadata: Metadados da execucao
        """
        source_id = f"src_{hashlib.md5(source.encode()).hexdigest()[:8]}"
        self.add_source(source_id, source, metadata)

        previous_id = source_id

        for i, transform in enumerate(transformations):
            transform_id = f"txf_{i}_{hashlib.md5(transform.encode()).hexdigest()[:8]}"
            self.add_transformation(transform_id, transform, metadata)
            self.add_relationship(previous_id, transform_id, 'transforms')
            previous_id = transform_id

        target_id = f"tgt_{hashlib.md5(target.encode()).hexdigest()[:8]}"
        self.add_target(target_id, target, metadata)
        self.add_relationship(previous_id, target_id, 'loads_to')

    def export_to_json(self) -> dict[str, Any]:
        """
        Exporta linhagem para JSON

        Returns:
            Dicionario com linhagem completa
        """
        return {
            'pipeline_id': self.pipeline_id,
            'timestamp': datetime.now().isoformat(),
            'nodes': [node.to_dict() for node in self.nodes.values()],
            'edges': [edge.to_dict() for edge in self.edges]
        }

    def save(self):
        """Salva linhagem em arquivo"""
        try:
            self.output_file.parent.mkdir(parents=True, exist_ok=True)

            lineage_data = self.export_to_json()

            with open(self.output_file, 'w') as f:
                json.dump(lineage_data, f, indent=2)

            logger.info(f"Linhagem salva em {self.output_file}")
        except Exception as e:
            logger.error(f"Erro ao salvar linhagem: {e}")
            raise

    def visualize_graph(self) -> str:
        """
        Gera representacao textual do grafo de linhagem

        Returns:
            String com representacao do grafo
        """
        lines = [f"Pipeline: {self.pipeline_id}", ""]

        for node in self.nodes.values():
            lines.append(f"[{node.node_type}] {node.name} ({node.node_id})")

        lines.append("")
        lines.append("Relacoes:")

        for edge in self.edges:
            source_name = self.nodes[edge.source_id].name
            target_name = self.nodes[edge.target_id].name
            lines.append(f"  {source_name} --[{edge.edge_type}]--> {target_name}")

        return "\n".join(lines)

