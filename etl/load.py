"""
Modulo de carga de dados para multiplos destinos
"""

import pandas as pd
from pathlib import Path
from typing import Union, Optional, Dict, Any


class DataLoader:
    """Carregador de dados para multiplos destinos"""

    def __init__(self):
        self.supported_formats = ['csv', 'json', 'excel', 'parquet']

    def load_to_csv(self, df: pd.DataFrame, file_path: Union[str, Path], index: bool = False, **kwargs) -> bool:
        """
        Carrega dados para arquivo CSV

        Args:
            df: DataFrame para carregar
            file_path: Caminho do arquivo de destino
            index: Se deve incluir index
            **kwargs: Parametros adicionais para to_csv

        Returns:
            True se sucesso
        """
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(path, index=index, **kwargs)
            return True
        except Exception as e:
            raise RuntimeError(f"Erro ao salvar CSV: {e}")

    def load_to_json(self, df: pd.DataFrame, file_path: Union[str, Path], orient: str = 'records', **kwargs) -> bool:
        """
        Carrega dados para arquivo JSON

        Args:
            df: DataFrame para carregar
            file_path: Caminho do arquivo de destino
            orient: Orientacao do JSON
            **kwargs: Parametros adicionais para to_json

        Returns:
            True se sucesso
        """
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_json(path, orient=orient, **kwargs)
            return True
        except Exception as e:
            raise RuntimeError(f"Erro ao salvar JSON: {e}")

    def load_to_excel(self, df: pd.DataFrame, file_path: Union[str, Path], sheet_name: str = 'Sheet1', index: bool = False, **kwargs) -> bool:
        """
        Carrega dados para arquivo Excel

        Args:
            df: DataFrame para carregar
            file_path: Caminho do arquivo de destino
            sheet_name: Nome da planilha
            index: Se deve incluir index
            **kwargs: Parametros adicionais para to_excel

        Returns:
            True se sucesso
        """
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_excel(path, sheet_name=sheet_name, index=index, **kwargs)
            return True
        except Exception as e:
            raise RuntimeError(f"Erro ao salvar Excel: {e}")

    def load_to_parquet(self, df: pd.DataFrame, file_path: Union[str, Path], **kwargs) -> bool:
        """
        Carrega dados para arquivo Parquet

        Args:
            df: DataFrame para carregar
            file_path: Caminho do arquivo de destino
            **kwargs: Parametros adicionais para to_parquet

        Returns:
            True se sucesso
        """
        try:
            path = Path(file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(path, **kwargs)
            return True
        except Exception as e:
            raise RuntimeError(f"Erro ao salvar Parquet: {e}")

    def load(self, df: pd.DataFrame, destination: Union[str, Path], format_type: str = 'csv', **kwargs) -> bool:
        """
        Metodo generico de carga

        Args:
            df: DataFrame para carregar
            destination: Destino (caminho de arquivo ou configuracao)
            format_type: Tipo de formato (csv, json, excel, parquet)
            **kwargs: Parametros adicionais

        Returns:
            True se sucesso
        """
        if format_type not in self.supported_formats:
            raise ValueError(f"Formato nao suportado: {format_type}")

        if format_type == 'csv':
            return self.load_to_csv(df, destination, **kwargs)
        elif format_type == 'json':
            return self.load_to_json(df, destination, **kwargs)
        elif format_type == 'excel':
            return self.load_to_excel(df, destination, **kwargs)
        elif format_type == 'parquet':
            return self.load_to_parquet(df, destination, **kwargs)

        raise NotImplementedError(f"Carga para {format_type} nao implementada")
