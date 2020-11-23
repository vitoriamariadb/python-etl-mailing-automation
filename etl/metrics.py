"""
Sistema de metricas para pipeline ETL
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from collections import defaultdict


class MetricsCollector:
    """Coletor de metricas de execucao"""

    def __init__(self):
        self.metrics = defaultdict(list)
        self.counters = defaultdict(int)
        self.timers = {}

    def record_metric(self, name: str, value: Any, timestamp: Optional[datetime] = None):
        """
        Registra uma metrica

        Args:
            name: Nome da metrica
            value: Valor da metrica
            timestamp: Timestamp (opcional)
        """
        metric_entry = {
            'value': value,
            'timestamp': timestamp or datetime.now()
        }
        self.metrics[name].append(metric_entry)

    def increment_counter(self, name: str, amount: int = 1):
        """
        Incrementa contador

        Args:
            name: Nome do contador
            amount: Quantidade a incrementar
        """
        self.counters[name] += amount

    def start_timer(self, name: str):
        """
        Inicia timer

        Args:
            name: Nome do timer
        """
        self.timers[name] = {'start': datetime.now(), 'end': None}

    def stop_timer(self, name: str) -> float:
        """
        Para timer e retorna duracao

        Args:
            name: Nome do timer

        Returns:
            Duracao em segundos
        """
        if name not in self.timers:
            raise ValueError(f"Timer {name} nao foi iniciado")

        self.timers[name]['end'] = datetime.now()
        duration = (self.timers[name]['end'] - self.timers[name]['start']).total_seconds()

        self.record_metric(f"{name}_duration", duration)

        return duration

    def get_metric_summary(self, name: str) -> Dict[str, Any]:
        """
        Retorna resumo de uma metrica

        Args:
            name: Nome da metrica

        Returns:
            Dicionario com resumo
        """
        if name not in self.metrics or not self.metrics[name]:
            return {}

        values = [m['value'] for m in self.metrics[name]]

        if not values:
            return {}

        numeric_values = [v for v in values if isinstance(v, (int, float))]

        if numeric_values:
            return {
                'count': len(numeric_values),
                'min': min(numeric_values),
                'max': max(numeric_values),
                'avg': sum(numeric_values) / len(numeric_values),
                'total': sum(numeric_values),
                'last': numeric_values[-1]
            }

        return {
            'count': len(values),
            'last': values[-1]
        }

    def get_all_metrics(self) -> Dict[str, Any]:
        """
        Retorna todas as metricas

        Returns:
            Dicionario com todas as metricas
        """
        return {
            'metrics': {name: self.get_metric_summary(name) for name in self.metrics},
            'counters': dict(self.counters),
            'active_timers': [name for name, timer in self.timers.items() if timer['end'] is None]
        }

    def reset(self):
        """Reseta todas as metricas"""
        self.metrics.clear()
        self.counters.clear()
        self.timers.clear()


class PipelineMetrics:
    """Metricas especificas de pipeline ETL"""

    def __init__(self):
        self.collector = MetricsCollector()

    def record_extraction(self, rows: int, duration: float, source: str):
        """Registra metricas de extracao"""
        self.collector.record_metric('extraction_rows', rows)
        self.collector.record_metric('extraction_duration', duration)
        self.collector.record_metric('extraction_source', source)
        self.collector.increment_counter('total_extractions')

    def record_transformation(self, rows_before: int, rows_after: int, duration: float, operation: str):
        """Registra metricas de transformacao"""
        self.collector.record_metric('transformation_rows_before', rows_before)
        self.collector.record_metric('transformation_rows_after', rows_after)
        self.collector.record_metric('transformation_duration', duration)
        self.collector.record_metric('transformation_operation', operation)
        self.collector.increment_counter('total_transformations')

    def record_load(self, rows: int, duration: float, destination: str):
        """Registra metricas de carga"""
        self.collector.record_metric('load_rows', rows)
        self.collector.record_metric('load_duration', duration)
        self.collector.record_metric('load_destination', destination)
        self.collector.increment_counter('total_loads')

    def record_pipeline_execution(self, status: str, duration: float, rows: int):
        """Registra metricas de execucao de pipeline"""
        self.collector.record_metric('pipeline_status', status)
        self.collector.record_metric('pipeline_duration', duration)
        self.collector.record_metric('pipeline_rows', rows)
        self.collector.increment_counter(f'pipeline_{status}')

    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Retorna resumo de performance

        Returns:
            Dicionario com resumo de performance
        """
        return {
            'extraction': {
                'total_rows': self.collector.get_metric_summary('extraction_rows').get('total', 0),
                'avg_duration': self.collector.get_metric_summary('extraction_duration').get('avg', 0),
                'count': self.collector.counters.get('total_extractions', 0)
            },
            'transformation': {
                'avg_duration': self.collector.get_metric_summary('transformation_duration').get('avg', 0),
                'count': self.collector.counters.get('total_transformations', 0)
            },
            'load': {
                'total_rows': self.collector.get_metric_summary('load_rows').get('total', 0),
                'avg_duration': self.collector.get_metric_summary('load_duration').get('avg', 0),
                'count': self.collector.counters.get('total_loads', 0)
            },
            'pipeline': {
                'success': self.collector.counters.get('pipeline_success', 0),
                'failed': self.collector.counters.get('pipeline_failed', 0),
                'avg_duration': self.collector.get_metric_summary('pipeline_duration').get('avg', 0)
            }
        }

    def reset(self):
        """Reseta metricas"""
        self.collector.reset()
