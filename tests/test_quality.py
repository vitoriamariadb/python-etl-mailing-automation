"""
Testes para modulo de qualidade de dados
"""

import pytest
import pandas as pd

from etl.quality import DataQualityRule, DataQualityValidator


class TestDataQuality:
    """Testes para validacao de qualidade de dados"""

    @pytest.fixture
    def sample_data(self):
        """Dados de exemplo para testes"""
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', None, 'David', 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com', 'david@test.com', 'eve@test.com']
        })

    def test_completeness_check_pass(self, sample_data):
        """Testa check de completude que passa"""
        validator = DataQualityValidator()
        validator.add_completeness_check(['age', 'email'], threshold=0.9)

        report = validator.validate(sample_data)

        assert report['passed'] == 1
        assert report['success_rate'] == 1.0

    def test_completeness_check_fail(self, sample_data):
        """Testa check de completude que falha"""
        validator = DataQualityValidator()
        validator.add_completeness_check(['name'], threshold=0.95)

        report = validator.validate(sample_data)

        assert report['failed'] == 1

    def test_uniqueness_check_pass(self, sample_data):
        """Testa check de unicidade que passa"""
        validator = DataQualityValidator()
        validator.add_uniqueness_check(['id'])

        report = validator.validate(sample_data)

        assert report['passed'] == 1

    def test_uniqueness_check_fail(self):
        """Testa check de unicidade que falha"""
        data = pd.DataFrame({
            'id': [1, 2, 2, 3],
            'name': ['A', 'B', 'C', 'D']
        })

        validator = DataQualityValidator()
        validator.add_uniqueness_check(['id'])

        report = validator.validate(data)

        assert report['failed'] == 1

    def test_range_check_pass(self, sample_data):
        """Testa check de range que passa"""
        validator = DataQualityValidator()
        validator.add_range_check('age', min_value=0, max_value=100)

        report = validator.validate(sample_data)

        assert report['passed'] == 1

    def test_range_check_fail_min(self):
        """Testa check de range que falha por minimo"""
        data = pd.DataFrame({'value': [-5, 10, 20]})

        validator = DataQualityValidator()
        validator.add_range_check('value', min_value=0)

        report = validator.validate(data)

        assert report['failed'] == 1

    def test_range_check_fail_max(self):
        """Testa check de range que falha por maximo"""
        data = pd.DataFrame({'value': [5, 10, 150]})

        validator = DataQualityValidator()
        validator.add_range_check('value', max_value=100)

        report = validator.validate(data)

        assert report['failed'] == 1

    def test_pattern_check_email_pass(self, sample_data):
        """Testa check de padrao de email que passa"""
        validator = DataQualityValidator()
        validator.add_pattern_check('email', r'^[\w\.-]+@[\w\.-]+\.\w+$')

        report = validator.validate(sample_data)

        assert report['passed'] == 1

    def test_pattern_check_fail(self):
        """Testa check de padrao que falha"""
        data = pd.DataFrame({'email': ['valid@test.com', 'invalid-email', 'another@test.com']})

        validator = DataQualityValidator()
        validator.add_pattern_check('email', r'^[\w\.-]+@[\w\.-]+\.\w+$')

        report = validator.validate(data)

        assert report['failed'] == 1

    def test_custom_check(self, sample_data):
        """Testa check customizado"""
        def check_age_positive(df):
            return (df['age'] > 0).all()

        validator = DataQualityValidator()
        validator.add_custom_check('positive_age', check_age_positive, 'Verifica se age e positivo')

        report = validator.validate(sample_data)

        assert report['passed'] == 1

    def test_multiple_rules(self, sample_data):
        """Testa multiplas regras"""
        validator = DataQualityValidator()
        validator.add_uniqueness_check(['id'])
        validator.add_range_check('age', min_value=0, max_value=100)
        validator.add_pattern_check('email', r'^[\w\.-]+@[\w\.-]+\.\w+$')

        report = validator.validate(sample_data)

        assert report['total_rules'] == 3
        assert report['passed'] == 3

    def test_get_failed_rules(self):
        """Testa recuperacao de regras que falharam"""
        data = pd.DataFrame({'id': [1, 1, 2]})

        validator = DataQualityValidator()
        validator.add_uniqueness_check(['id'])

        validator.validate(data)
        failed = validator.get_failed_rules()

        assert len(failed) == 1
        assert failed[0]['rule'] == 'uniqueness_id'

    def test_data_quality_rule(self):
        """Testa classe DataQualityRule diretamente"""
        def check_func(df):
            return len(df) > 0

        rule = DataQualityRule('test_rule', check_func, 'Test description')

        df = pd.DataFrame({'col': [1, 2, 3]})
        result = rule.validate(df)

        assert result['passed'] is True
        assert result['rule'] == 'test_rule'
        assert 'timestamp' in result
