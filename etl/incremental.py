"""
Gerenciamento de carga incremental
"""

import json
import pandas as pd
from pathlib import Path
from typing import Any
from datetime import datetime

from etl.logger import get_logger

logger = get_logger('incremental')

class IncrementalLoadManager:
    """Gerencia estado de cargas incrementais"""

    def __init__(self, state_file: str | Path = 'state/incremental_state.json'):
        self.state_file = Path(state_file)
        self.state: dict[str, Any] = {}
        self._load_state()

    def _load_state(self):
        """Carrega estado de arquivo"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    self.state = json.load(f)
                logger.info(f"Estado carregado: {len(self.state)} entradas")
            except Exception as e:
                logger.error(f"Erro ao carregar estado: {e}")
                self.state = {}
        else:
            logger.info("Nenhum estado anterior encontrado")
            self.state = {}

    def _save_state(self):
        """Salva estado em arquivo"""
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2, default=str)
            logger.info("Estado salvo com sucesso")
        except Exception as e:
            logger.error(f"Erro ao salvar estado: {e}")
            raise

    def get_last_value(self, key: str) -> Any | None:
        """
        Recupera ultimo valor para uma chave

        Args:
            key: Chave do estado

        Returns:
            Ultimo valor ou None
        """
        return self.state.get(key)

    def update_state(self, key: str, value: Any):
        """
        Atualiza estado para uma chave

        Args:
            key: Chave do estado
            value: Novo valor
        """
        self.state[key] = value
        self._save_state()

    def get_incremental_data(
        self,
        df: pd.DataFrame,
        key_column: str,
        comparison_column: str,
        state_key: str
    ) -> pd.DataFrame:
        """
        Filtra dados incrementais baseado em coluna de comparacao

        Args:
            df: DataFrame completo
            key_column: Coluna chave unica
            comparison_column: Coluna para comparacao (ex: timestamp, id)
            state_key: Chave para salvar estado

        Returns:
            DataFrame com apenas dados novos
        """
        last_value = self.get_last_value(state_key)

        if last_value is None:
            logger.info("Primeira carga - processando todos os dados")
            incremental_df = df.copy()
        else:
            logger.info(f"Filtrando dados desde: {last_value}")
            if pd.api.types.is_datetime64_any_dtype(df[comparison_column]):
                last_value = pd.to_datetime(last_value)

            incremental_df = df[df[comparison_column] > last_value].copy()
            logger.info(f"Encontrados {len(incremental_df)} novos registros")

        if not incremental_df.empty:
            new_max_value = incremental_df[comparison_column].max()
            self.update_state(state_key, new_max_value)
            logger.info(f"Novo valor maximo: {new_max_value}")

        return incremental_df

class CDCProcessor:
    """Processador de Change Data Capture"""

    def __init__(self):
        self.changes: dict[str, list] = {
            'inserts': [],
            'updates': [],
            'deletes': []
        }

    def detect_changes(
        self,
        current_df: pd.DataFrame,
        previous_df: pd.DataFrame,
        key_column: str
    ) -> dict[str, pd.DataFrame]:
        """
        Detecta mudancas entre datasets

        Args:
            current_df: DataFrame atual
            previous_df: DataFrame anterior
            key_column: Coluna chave para comparacao

        Returns:
            Dicionario com inserts, updates, deletes
        """
        current_keys = set(current_df[key_column])
        previous_keys = set(previous_df[key_column])

        insert_keys = current_keys - previous_keys
        delete_keys = previous_keys - current_keys
        potential_update_keys = current_keys & previous_keys

        inserts = current_df[current_df[key_column].isin(insert_keys)]

        deletes = previous_df[previous_df[key_column].isin(delete_keys)]

        updates = pd.DataFrame()
        if potential_update_keys:
            current_subset = current_df[current_df[key_column].isin(potential_update_keys)]
            previous_subset = previous_df[previous_df[key_column].isin(potential_update_keys)]

            merged = current_subset.merge(
                previous_subset,
                on=key_column,
                suffixes=('_current', '_previous')
            )

            changed_rows = []
            for _, row in merged.iterrows():
                current_cols = [col for col in merged.columns if col.endswith('_current')]
                previous_cols = [col for col in merged.columns if col.endswith('_previous')]

                for curr_col, prev_col in zip(current_cols, previous_cols):
                    if row[curr_col] != row[prev_col]:
                        changed_rows.append(row[key_column])
                        break

            if changed_rows:
                updates = current_df[current_df[key_column].isin(changed_rows)]

        return {
            'inserts': inserts,
            'updates': updates,
            'deletes': deletes
        }

    def get_summary(self, changes: dict[str, pd.DataFrame]) -> dict[str, int]:
        """Retorna sumario de mudancas"""
        return {
            'inserts': len(changes['inserts']),
            'updates': len(changes['updates']),
            'deletes': len(changes['deletes'])
        }
