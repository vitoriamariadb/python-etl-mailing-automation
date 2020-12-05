"""
Testes para modulo de carga
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import json

from etl.load import DataLoader


@pytest.fixture
def loader():
    """Fixture do loader"""
    return DataLoader()


@pytest.fixture
def sample_df():
    """DataFrame de exemplo"""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [100, 200, 300]
    })


class TestDataLoader:

    def test_load_to_csv(self, loader, sample_df):
        """Testa carga para CSV"""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / 'output.csv'
            result = loader.load_to_csv(sample_df, output_path)

            assert result is True
            assert output_path.exists()

            df_loaded = pd.read_csv(output_path)
            assert len(df_loaded) == 3
            assert list(df_loaded.columns) == ['id', 'name', 'value']

    def test_load_to_json(self, loader, sample_df):
        """Testa carga para JSON"""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / 'output.json'
            result = loader.load_to_json(sample_df, output_path)

            assert result is True
            assert output_path.exists()

            with open(output_path) as f:
                data = json.load(f)
            assert len(data) == 3

    def test_load_to_csv_creates_directory(self, loader, sample_df):
        """Testa que diretorio e criado automaticamente"""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / 'subdir' / 'output.csv'
            result = loader.load_to_csv(sample_df, output_path)

            assert result is True
            assert output_path.exists()
            assert output_path.parent.exists()

    def test_load_generic_csv(self, loader, sample_df):
        """Testa metodo generico de carga para CSV"""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / 'output.csv'
            result = loader.load(sample_df, output_path, format_type='csv')

            assert result is True
            assert output_path.exists()

    def test_load_generic_json(self, loader, sample_df):
        """Testa metodo generico de carga para JSON"""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / 'output.json'
            result = loader.load(sample_df, output_path, format_type='json')

            assert result is True
            assert output_path.exists()

    def test_load_unsupported_format(self, loader, sample_df):
        """Testa erro para formato nao suportado"""
        with pytest.raises(ValueError):
            loader.load(sample_df, 'output.xyz', format_type='xyz')

    def test_supported_formats(self, loader):
        """Testa lista de formatos suportados"""
        assert 'csv' in loader.supported_formats
        assert 'json' in loader.supported_formats
        assert 'excel' in loader.supported_formats
        assert 'parquet' in loader.supported_formats
