# Python ETL Pipeline

Pipeline ETL modular e robusto para extração, transformação e carga de dados.

## Recursos

- Extração de múltiplas fontes (CSV, JSON, Excel)
- Transformações customizáveis
- Carga para diversos formatos
- Logging estruturado
- Sistema de retry
- Validação de dados
- Métricas de execução
- Error handling robusto

## Estrutura

```
etl/
├── extract.py       # Extração de dados
├── transform.py     # Transformação de dados
├── load.py          # Carga de dados
├── pipeline.py      # Orquestrador de pipeline
├── logger.py        # Sistema de logging
├── validators.py    # Validadores de dados
├── error_handler.py # Tratamento de erros
├── retry.py         # Sistema de retry
├── metrics.py       # Métricas de execução
└── exceptions.py    # Exceções customizadas

tests/
├── test_extract.py     # Testes de extração
├── test_transform.py   # Testes de transformação
└── test_load.py        # Testes de carga
```

## Uso Básico

### Extração de Dados

```python
from etl.extract import DataExtractor

extractor = DataExtractor()
df = extractor.extract('data.csv', source_type='csv')
```

### Transformação de Dados

```python
from etl.transform import DataTransformer

transformer = DataTransformer()
df_clean = transformer.remove_duplicates(df)
df_clean = transformer.remove_null_rows(df_clean)
```

### Carga de Dados

```python
from etl.load import DataLoader

loader = DataLoader()
loader.load(df_clean, 'output.csv', format_type='csv')
```

### Pipeline Completo

```python
from etl.pipeline import ETLPipeline

pipeline = ETLPipeline(name="Meu Pipeline")

transformations = [
    lambda df: transformer.remove_duplicates(df),
    lambda df: transformer.remove_null_rows(df)
]

result = pipeline.run(
    source='input.csv',
    destination='output.csv',
    transformations=transformations,
    source_type='csv',
    dest_type='csv'
)

print(f"Status: {result['status']}")
print(f"Linhas processadas: {result['rows_processed']}")
```

## Logging

O sistema possui logging estruturado rotacionado:

```python
from etl.logger import get_logger

logger = get_logger('meu_etl')
logger.info("Processamento iniciado")
logger.log_pipeline_execution("Pipeline X", duration=10.5, rows=1000)
```

Logs são salvos em `logs/etl.log` com rotação automática.

## Validação de Dados

```python
from etl.validators import DataValidator

validator = DataValidator()

rules = [
    {'type': 'not_null', 'columns': ['id', 'name']},
    {'type': 'unique', 'columns': ['id']},
    {'type': 'data_type', 'column_types': {'id': 'int', 'name': 'string'}}
]

result = validator.validate_all(df, rules)
print(f"Validação: {result['all_passed']}")
```

## Retry

Sistema de retry com exponential backoff:

```python
from etl.retry import RetryConfig, retry_operation

config = RetryConfig(max_attempts=3, delay=1.0, backoff_factor=2.0)

@retry_operation(config)
def operacao_com_retry():
    # operação que pode falhar
    pass
```

## Métricas

Coleta de métricas de execução:

```python
from etl.metrics import PipelineMetrics

metrics = PipelineMetrics()
metrics.record_extraction(rows=1000, duration=2.5, source='data.csv')
summary = metrics.get_performance_summary()
```

## Testes

Execute os testes:

```bash
pytest tests/ -v
```

## Tecnologias

- Python 3.8+
- pandas (manipulação de dados)
- pytest (testes)

## Licença

MIT
