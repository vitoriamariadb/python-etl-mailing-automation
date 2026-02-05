# -*- coding: utf-8 -*-
"""
Profiler Module
Analise estatistica e perfilamento de dados para pipelines ETL.
"""

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ColumnProfile:
    """Perfil estatistico de uma coluna."""

    name: str
    dtype: str
    total_count: int = 0
    null_count: int = 0
    unique_count: int = 0
    min_value: Any = None
    max_value: Any = None
    mean_value: float | None = None
    median_value: float | None = None
    std_value: float | None = None
    most_common: list[tuple[Any, int]] = field(default_factory=list)
    sample_values: list[Any] = field(default_factory=list)

    @property
    def null_rate(self) -> float:
        """Taxa de valores nulos na coluna."""
        if self.total_count == 0:
            return 0.0
        return self.null_count / self.total_count

    @property
    def fill_rate(self) -> float:
        """Taxa de preenchimento da coluna."""
        return 1.0 - self.null_rate

    @property
    def unique_rate(self) -> float:
        """Taxa de valores unicos na coluna."""
        if self.total_count == 0:
            return 0.0
        return self.unique_count / self.total_count


@dataclass
class DataProfile:
    """Perfil completo de um dataset."""

    source: str
    row_count: int = 0
    column_count: int = 0
    columns: list[ColumnProfile] = field(default_factory=list)
    profiled_at: str = field(default_factory=lambda: datetime.now().isoformat())
    warnings: list[str] = field(default_factory=list)

    def get_column(self, name: str) -> ColumnProfile | None:
        """Retorna perfil de uma coluna pelo nome."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def get_high_null_columns(self, threshold: float = 0.5) -> list[ColumnProfile]:
        """Retorna colunas com taxa de nulos acima do limiar."""
        return [c for c in self.columns if c.null_rate > threshold]

    def get_low_cardinality_columns(self, threshold: int = 10) -> list[ColumnProfile]:
        """Retorna colunas com baixa cardinalidade."""
        return [c for c in self.columns if c.unique_count <= threshold]

    def to_dict(self) -> dict[str, Any]:
        """Serializa perfil para dicionario."""
        return {
            "source": self.source,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "profiled_at": self.profiled_at,
            "columns": [
                {
                    "name": c.name,
                    "dtype": c.dtype,
                    "null_rate": round(c.null_rate, 4),
                    "fill_rate": round(c.fill_rate, 4),
                    "unique_count": c.unique_count,
                    "min": str(c.min_value) if c.min_value is not None else None,
                    "max": str(c.max_value) if c.max_value is not None else None,
                }
                for c in self.columns
            ],
            "warnings": self.warnings,
        }



class DataProfiler:
    """Perfilador de dados com suporte a multiplas fontes."""

    def __init__(self, sample_size: int = 5, top_n: int = 10) -> None:
        self._sample_size = sample_size
        self._top_n = top_n

    def profile(self, data: list[dict[str, Any]], source: str = "unknown") -> DataProfile:
        """Gera perfil completo de um dataset."""
        if not data:
            logger.warning("Dataset vazio recebido para profiling")
            return DataProfile(source=source)

        profile = DataProfile(
            source=source,
            row_count=len(data),
            column_count=len(data[0]),
        )

        columns = list(data[0].keys())
        for col_name in columns:
            col_profile = self._profile_column(data, col_name)
            profile.columns.append(col_profile)

        self._generate_warnings(profile)
        logger.info(
            "Profiling concluido: %s (%d linhas, %d colunas)",
            source,
            profile.row_count,
            profile.column_count,
        )
        return profile

    def _profile_column(self, data: list[dict[str, Any]], col_name: str) -> ColumnProfile:
        """Gera perfil de uma coluna especifica."""
        values = [row.get(col_name) for row in data]
        non_null = [v for v in values if v is not None]

        profile = ColumnProfile(
            name=col_name,
            dtype=type(non_null[0]).__name__ if non_null else "unknown",
            total_count=len(values),
            null_count=sum(1 for v in values if v is None),
            unique_count=len(set(str(v) for v in non_null)),
        )

        if non_null:
            profile.sample_values = non_null[: self._sample_size]

            try:
                numeric = [float(v) for v in non_null]
                profile.min_value = min(numeric)
                profile.max_value = max(numeric)
                profile.mean_value = sum(numeric) / len(numeric)
                sorted_nums = sorted(numeric)
                mid = len(sorted_nums) // 2
                if len(sorted_nums) % 2 == 0:
                    profile.median_value = (sorted_nums[mid - 1] + sorted_nums[mid]) / 2
                else:
                    profile.median_value = sorted_nums[mid]
                mean = profile.mean_value
                variance = sum((x - mean) ** 2 for x in numeric) / len(numeric)
                profile.std_value = variance**0.5
            except (ValueError, TypeError):
                str_values = [str(v) for v in non_null]
                profile.min_value = min(str_values)
                profile.max_value = max(str_values)

            counter = Counter(str(v) for v in non_null)
            profile.most_common = counter.most_common(self._top_n)

        return profile

    def _generate_warnings(self, profile: DataProfile) -> None:
        """Gera avisos automaticos baseados no perfil.

        Analisa colunas com alta taxa de nulos, valores constantes
        e possíveis identificadores unicos.
        """
        for col in profile.columns:
            if col.null_rate > 0.5:
                profile.warnings.append(f"Coluna '{col.name}' tem {col.null_rate:.0%} de valores nulos")
            if col.unique_count == 1 and col.total_count > 1:
                profile.warnings.append(f"Coluna '{col.name}' tem valor constante")
            if col.unique_rate == 1.0 and col.total_count > 10:
                profile.warnings.append(f"Coluna '{col.name}' pode ser identificador (100% unico)")


# "O homem que move montanhas comeca carregando pequenas pedras." -- Confucio
