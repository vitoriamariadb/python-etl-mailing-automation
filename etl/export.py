"""
Exportacao de dados para multiplos formatos
"""

import pandas as pd
from pathlib import Path
from typing import Any
from datetime import datetime

from etl.logger import get_logger

logger = get_logger('export')

class DataExporter:
    """Exportador de dados para multiplos formatos"""

    def __init__(self):
        self.supported_formats = ['csv', 'parquet', 'json', 'excel', 'html', 'markdown']

    def export_csv(
        self,
        df: pd.DataFrame,
        file_path: str | Path,
        separator: str = ',',
        compression: str | None = None,
        **kwargs
    ) -> bool:
        """
        Exporta para CSV

        Args:
            df: DataFrame para exportar
            file_path: Caminho do arquivo
            separator: Separador de campos
            compression: Tipo de compressao (gzip, bz2, zip)
            **kwargs: Parametros adicionais

        Returns:
            True se sucesso
        """
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            df.to_csv(path, sep=separator, index=False, compression=compression, **kwargs)
            logger.info(f"CSV exportado: {path}")
            return True

        except Exception as e:
            logger.error(f"Erro ao exportar CSV: {e}")
            raise

    def export_parquet(
        self,
        df: pd.DataFrame,
        file_path: str | Path,
        compression: str = 'snappy',
        partition_cols: list[str | None] = None,
        **kwargs
    ) -> bool:
        """
        Exporta para Parquet

        Args:
            df: DataFrame para exportar
            file_path: Caminho do arquivo
            compression: Tipo de compressao (snappy, gzip, brotli)
            partition_cols: Colunas para particionar
            **kwargs: Parametros adicionais

        Returns:
            True se sucesso
        """
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            df.to_parquet(
                path,
                compression=compression,
                partition_cols=partition_cols,
                index=False,
                **kwargs
            )
            logger.info(f"Parquet exportado: {path}")
            return True

        except Exception as e:
            logger.error(f"Erro ao exportar Parquet: {e}")
            raise

    def export_json(
        self,
        df: pd.DataFrame,
        file_path: str | Path,
        orient: str = 'records',
        indent: int | None = 2,
        **kwargs
    ) -> bool:
        """
        Exporta para JSON

        Args:
            df: DataFrame para exportar
            file_path: Caminho do arquivo
            orient: Orientacao do JSON
            indent: Indentacao
            **kwargs: Parametros adicionais

        Returns:
            True se sucesso
        """
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            df.to_json(path, orient=orient, indent=indent, **kwargs)
            logger.info(f"JSON exportado: {path}")
            return True

        except Exception as e:
            logger.error(f"Erro ao exportar JSON: {e}")
            raise

    def export_excel(
        self,
        df: pd.DataFrame,
        file_path: str | Path,
        sheet_name: str = 'Sheet1',
        **kwargs
    ) -> bool:
        """
        Exporta para Excel

        Args:
            df: DataFrame para exportar
            file_path: Caminho do arquivo
            sheet_name: Nome da planilha
            **kwargs: Parametros adicionais

        Returns:
            True se sucesso
        """
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            df.to_excel(path, sheet_name=sheet_name, index=False, **kwargs)
            logger.info(f"Excel exportado: {path}")
            return True

        except Exception as e:
            logger.error(f"Erro ao exportar Excel: {e}")
            raise

    def export_html(
        self,
        df: pd.DataFrame,
        file_path: str | Path,
        **kwargs
    ) -> bool:
        """
        Exporta para HTML

        Args:
            df: DataFrame para exportar
            file_path: Caminho do arquivo
            **kwargs: Parametros adicionais

        Returns:
            True se sucesso
        """
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            df.to_html(path, index=False, **kwargs)
            logger.info(f"HTML exportado: {path}")
            return True

        except Exception as e:
            logger.error(f"Erro ao exportar HTML: {e}")
            raise

    def export_markdown(
        self,
        df: pd.DataFrame,
        file_path: str | Path,
        **kwargs
    ) -> bool:
        """
        Exporta para Markdown

        Args:
            df: DataFrame para exportar
            file_path: Caminho do arquivo
            **kwargs: Parametros adicionais

        Returns:
            True se sucesso
        """
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            md_content = df.to_markdown(index=False, **kwargs)
            with open(path, 'w') as f:
                f.write(md_content)

            logger.info(f"Markdown exportado: {path}")
            return True

        except Exception as e:
            logger.error(f"Erro ao exportar Markdown: {e}")
            raise

    def export(
        self,
        df: pd.DataFrame,
        file_path: str | Path,
        format_type: str,
        **kwargs
    ) -> bool:
        """
        Exporta para formato especificado

        Args:
            df: DataFrame para exportar
            file_path: Caminho do arquivo
            format_type: Formato (csv, parquet, json, excel)
            **kwargs: Parametros adicionais

        Returns:
            True se sucesso
        """
        if format_type not in self.supported_formats:
            raise ValueError(f"Formato nao suportado: {format_type}")

        exporters = {
            'csv': self.export_csv,
            'parquet': self.export_parquet,
            'json': self.export_json,
            'excel': self.export_excel,
            'html': self.export_html,
            'markdown': self.export_markdown
        }

        exporter = exporters[format_type]
        return exporter(df, file_path, **kwargs)

    def export_multiple_formats(
        self,
        df: pd.DataFrame,
        base_path: str | Path,
        formats: list[str],
        **kwargs
    ) -> dict[str, bool]:
        """
        Exporta para multiplos formatos

        Args:
            df: DataFrame para exportar
            base_path: Caminho base (sem extensao)
            formats: Lista de formatos
            **kwargs: Parametros adicionais

        Returns:
            Dicionario com resultados por formato
        """
        base = Path(base_path)
        results = {}

        for fmt in formats:
            if fmt not in self.supported_formats:
                logger.warning(f"Formato nao suportado: {fmt}")
                results[fmt] = False
                continue

            file_path = base.parent / f"{base.stem}.{fmt}"

            try:
                result = self.export(df, file_path, fmt, **kwargs)
                results[fmt] = result
            except Exception as e:
                logger.error(f"Erro ao exportar {fmt}: {e}")
                results[fmt] = False

        return results

class PartitionedExporter:
    """Exporta dados particionados"""

    def __init__(self, base_path: str | Path):
        self.base_path = Path(base_path)

    def export_by_column(
        self,
        df: pd.DataFrame,
        partition_column: str,
        format_type: str = 'parquet',
        **kwargs
    ) -> dict[str, str]:
        """
        Exporta dados particionados por coluna

        Args:
            df: DataFrame para exportar
            partition_column: Coluna para particionar
            format_type: Formato de exportacao
            **kwargs: Parametros adicionais

        Returns:
            Dicionario com paths dos arquivos criados
        """
        if partition_column not in df.columns:
            raise ValueError(f"Coluna nao encontrada: {partition_column}")

        exporter = DataExporter()
        results = {}

        for value in df[partition_column].unique():
            subset = df[df[partition_column] == value]
            partition_dir = self.base_path / f"{partition_column}={value}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            file_path = partition_dir / f"data.{format_type}"
            exporter.export(subset, file_path, format_type, **kwargs)

            results[str(value)] = str(file_path)

        logger.info(f"Exportadas {len(results)} particoes")
        return results
