"""
Testes para modulo de extracao
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import json

from etl.extract import DataExtractor


@pytest.fixture
def extractor():
    """Fixture do extrator"""
    return DataExtractor()


@pytest.fixture
def sample_csv():
    """Cria arquivo CSV temporario para teste"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('id,name,value\n')
        f.write('1,Alice,100\n')
        f.write('2,Bob,200\n')
        f.write('3,Charlie,300\n')
        return f.name


@pytest.fixture
def sample_json():
    """Cria arquivo JSON temporario para teste"""
    data = [
        {'id': 1, 'name': 'Alice', 'value': 100},
        {'id': 2, 'name': 'Bob', 'value': 200},
        {'id': 3, 'name': 'Charlie', 'value': 300}
    ]
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(data, f)
        return f.name


class TestDataExtractor:

    def test_extract_from_csv(self, extractor, sample_csv):
        """Testa extracao de CSV"""
        df = extractor.extract_from_csv(sample_csv)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ['id', 'name', 'value']
        assert df['name'].tolist() == ['Alice', 'Bob', 'Charlie']

    def test_extract_from_csv_file_not_found(self, extractor):
        """Testa erro quando arquivo nao existe"""
        with pytest.raises(FileNotFoundError):
            extractor.extract_from_csv('nonexistent.csv')

    def test_extract_from_json(self, extractor, sample_json):
        """Testa extracao de JSON"""
        df = extractor.extract_from_json(sample_json)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert 'id' in df.columns
        assert 'name' in df.columns
        assert 'value' in df.columns

    def test_extract_generic_csv(self, extractor, sample_csv):
        """Testa metodo generico de extracao para CSV"""
        df = extractor.extract(sample_csv, source_type='csv')

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_extract_generic_json(self, extractor, sample_json):
        """Testa metodo generico de extracao para JSON"""
        df = extractor.extract(sample_json, source_type='json')

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    def test_extract_unsupported_format(self, extractor):
        """Testa erro para formato nao suportado"""
        with pytest.raises(ValueError):
            extractor.extract('file.xyz', source_type='xyz')

    def test_supported_formats(self, extractor):
        """Testa lista de formatos suportados"""
        assert 'csv' in extractor.supported_formats
        assert 'json' in extractor.supported_formats
        assert 'excel' in extractor.supported_formats


@pytest.fixture(autouse=True)
def cleanup():
    """Cleanup de arquivos temporarios"""
    yield
