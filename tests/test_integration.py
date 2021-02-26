"""
Testes de integracao do pipeline ETL
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import shutil

from etl.orchestrator import ETLOrchestrator
from etl.connectors import ConnectorFactory
from etl.processors import CleaningProcessor, TransformationProcessor
from etl.quality import DataQualityValidator
from etl.schema import TableSchema, ColumnSchema, DataType


class TestETLIntegration:
    """Testes de integracao end-to-end"""

    @pytest.fixture
    def temp_dir(self):
        """Cria diretorio temporario para testes"""
        temp_path = Path(tempfile.mkdtemp())
        yield temp_path
        shutil.rmtree(temp_path)

    @pytest.fixture
    def sample_data(self):
        """Cria dados de exemplo"""
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'city': ['SP', 'RJ', 'MG', 'RS', 'BA']
        })

    def test_csv_to_csv_pipeline(self, temp_dir, sample_data):
        """Testa pipeline CSV para CSV"""
        source_file = temp_dir / 'input.csv'
        dest_file = temp_dir / 'output.csv'

        sample_data.to_csv(source_file, index=False)

        orchestrator = ETLOrchestrator("CSV Pipeline")
        result = orchestrator.execute(
            source=str(source_file),
            destination=str(dest_file),
            source_type='csv',
            dest_type='csv'
        )

        assert result['status'] == 'success'
        assert dest_file.exists()

        output_data = pd.read_csv(dest_file)
        assert len(output_data) == len(sample_data)

    def test_pipeline_with_transformations(self, temp_dir, sample_data):
        """Testa pipeline com transformacoes"""
        source_file = temp_dir / 'input.csv'
        dest_file = temp_dir / 'output.csv'

        data_with_duplicates = pd.concat([sample_data, sample_data.iloc[:2]], ignore_index=True)
        data_with_duplicates.to_csv(source_file, index=False)

        orchestrator = ETLOrchestrator("Transform Pipeline")
        result = orchestrator.execute(
            source=str(source_file),
            destination=str(dest_file),
            source_type='csv',
            dest_type='csv',
            remove_duplicates=True
        )

        assert result['status'] == 'success'

        output_data = pd.read_csv(dest_file)
        assert len(output_data) == len(sample_data)

    def test_json_to_parquet_pipeline(self, temp_dir, sample_data):
        """Testa pipeline JSON para Parquet"""
        source_file = temp_dir / 'input.json'
        dest_file = temp_dir / 'output.parquet'

        sample_data.to_json(source_file, orient='records')

        orchestrator = ETLOrchestrator("JSON to Parquet")
        result = orchestrator.execute(
            source=str(source_file),
            destination=str(dest_file),
            source_type='json',
            dest_type='parquet'
        )

        assert result['status'] == 'success'
        assert dest_file.exists()

        output_data = pd.read_parquet(dest_file)
        assert len(output_data) == len(sample_data)

    def test_connector_factory(self):
        """Testa factory de conectores"""
        csv_connector = ConnectorFactory.create('csv')
        assert csv_connector is not None

        json_connector = ConnectorFactory.create('json')
        assert json_connector is not None

        parquet_connector = ConnectorFactory.create('parquet')
        assert parquet_connector is not None

    def test_quality_validation_integration(self, sample_data):
        """Testa validacao de qualidade integrada"""
        validator = DataQualityValidator()
        validator.add_completeness_check(['id', 'name'], threshold=0.9)
        validator.add_uniqueness_check(['id'])

        report = validator.validate(sample_data)

        assert report['total_rules'] == 2
        assert report['passed'] == 2
        assert report['success_rate'] == 1.0

    def test_schema_validation_integration(self, sample_data):
        """Testa validacao de schema integrada"""
        schema = TableSchema('test_table')
        schema.add_column(ColumnSchema('id', DataType.INTEGER, nullable=False, unique=True))
        schema.add_column(ColumnSchema('name', DataType.STRING, nullable=False))
        schema.add_column(ColumnSchema('age', DataType.INTEGER, nullable=False))

        result = schema.validate(sample_data)

        assert result['valid'] is True
        assert len(result['errors']) == 0

    def test_cleaning_processor(self, sample_data):
        """Testa processador de limpeza"""
        data_with_nulls = sample_data.copy()
        data_with_nulls.loc[2, 'name'] = None

        processor = CleaningProcessor(remove_nulls=True)
        cleaned = processor.process(data_with_nulls)

        assert len(cleaned) == len(sample_data) - 1
        assert cleaned['name'].isna().sum() == 0

    def test_transformation_processor(self, sample_data):
        """Testa processador de transformacoes"""
        def uppercase_names(df):
            df['name'] = df['name'].str.upper()
            return df

        processor = TransformationProcessor([uppercase_names])
        transformed = processor.process(sample_data)

        assert all(transformed['name'].str.isupper())

    def test_pipeline_error_handling(self, temp_dir):
        """Testa tratamento de erros no pipeline"""
        source_file = temp_dir / 'nonexistent.csv'
        dest_file = temp_dir / 'output.csv'

        orchestrator = ETLOrchestrator("Error Pipeline")
        result = orchestrator.execute(
            source=str(source_file),
            destination=str(dest_file),
            source_type='csv',
            dest_type='csv'
        )

        assert result['status'] == 'failed'
        assert 'error' in result
