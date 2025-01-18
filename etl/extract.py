"""
Modulo de extracao de dados de multiplas fontes
"""

import pandas as pd
from pathlib import Path
from typing import Any

class DataExtractor:
    """Extrator de dados de multiplas fontes"""

    def __init__(self):
        self.supported_formats = ['csv', 'json', 'excel']

    def extract_from_csv(self, file_path: str | Path, **kwargs) -> pd.DataFrame:
        """
        Extrai dados de arquivo CSV

        Args:
            file_path: Caminho do arquivo CSV
            **kwargs: Parametros adicionais para pd.read_csv

        Returns:
            DataFrame com os dados extraidos
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Arquivo nao encontrado: {file_path}")

        return pd.read_csv(path, **kwargs)

    def extract_from_json(self, file_path: str | Path, **kwargs) -> pd.DataFrame:
        """
        Extrai dados de arquivo JSON

        Args:
            file_path: Caminho do arquivo JSON
            **kwargs: Parametros adicionais para pd.read_json

        Returns:
            DataFrame com os dados extraidos
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Arquivo nao encontrado: {file_path}")

        return pd.read_json(path, **kwargs)

    def extract_from_excel(self, file_path: str | Path, sheet_name: str = 0, **kwargs) -> pd.DataFrame:
        """
        Extrai dados de arquivo Excel

        Args:
            file_path: Caminho do arquivo Excel
            sheet_name: Nome ou indice da planilha
            **kwargs: Parametros adicionais para pd.read_excel

        Returns:
            DataFrame com os dados extraidos
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Arquivo nao encontrado: {file_path}")

        return pd.read_excel(path, sheet_name=sheet_name, **kwargs)

    def extract(self, source: str | Path | dict[str, Any], source_type: str = 'csv', **kwargs) -> pd.DataFrame:
        """
        Metodo generico de extracao

        Args:
            source: Fonte de dados (caminho de arquivo ou configuracao)
            source_type: Tipo de fonte (csv, json, excel)
            **kwargs: Parametros adicionais

        Returns:
            DataFrame com os dados extraidos
        """
        if source_type not in self.supported_formats:
            raise ValueError(f"Formato nao suportado: {source_type}")

        if source_type == 'csv':
            return self.extract_from_csv(source, **kwargs)
        elif source_type == 'json':
            return self.extract_from_json(source, **kwargs)
        elif source_type == 'excel':
            return self.extract_from_excel(source, **kwargs)

        raise NotImplementedError(f"Extracao para {source_type} nao implementada")
