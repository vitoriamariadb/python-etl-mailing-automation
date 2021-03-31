# Pipeline ETL Avancado

Sistema ETL modular e extensivel para processamento de dados em larga escala.

## Caracteristicas

- Suporte a multiplas fontes de dados (CSV, JSON, Excel, Parquet, PostgreSQL, MySQL)
- Processamento paralelo e batch otimizado
- Validacao de qualidade e schema
- Carga incremental e CDC
- Data lineage tracking
- Sistema de checkpoint e recuperacao
- Exportacao para multiplos formatos
- Testes completos (80%+ cobertura)

## Instalacao

```bash
pip install -r requirements.txt
```

## Uso Basico

### Pipeline Simples

```python
from etl.orchestrator import ETLOrchestrator

orchestrator = ETLOrchestrator("Meu Pipeline")
result = orchestrator.execute(
    source='data/input.csv',
    destination='output/processed.csv',
    source_type='csv',
    dest_type='csv'
)

print(f"Status: {result['status']}")
print(f"Linhas processadas: {result['rows_processed']}")
```

### Pipeline com Transformacoes

```python
from etl.orchestrator import ETLOrchestrator

def remover_duplicatas(df):
    return df.drop_duplicates()

def filtrar_ativos(df):
    return df[df['ativo'] == True]

orchestrator = ETLOrchestrator("Pipeline Transformado")
result = orchestrator.execute(
    source='data/usuarios.csv',
    destination='output/usuarios_limpos.parquet',
    source_type='csv',
    dest_type='parquet',
    transformations=[remover_duplicatas, filtrar_ativos]
)
```

### Validacao de Qualidade

```python
from etl.quality import DataQualityValidator
import pandas as pd

df = pd.read_csv('data/clientes.csv')

validator = DataQualityValidator()
validator.add_completeness_check(['email', 'telefone'], threshold=0.95)
validator.add_uniqueness_check(['cpf'])
validator.add_pattern_check('email', r'^[\w\.-]+@[\w\.-]+\.\w+$')

report = validator.validate(df)
print(f"Taxa de sucesso: {report['success_rate']:.1%}")
```

### Validacao de Schema

```python
from etl.schema import TableSchema, ColumnSchema, DataType

schema = TableSchema('usuarios')
schema.add_column(ColumnSchema('id', DataType.INTEGER, nullable=False, unique=True))
schema.add_column(ColumnSchema('nome', DataType.STRING, nullable=False))
schema.add_column(ColumnSchema('idade', DataType.INTEGER, min_value=0, max_value=120))

result = schema.validate(df)
if not result['valid']:
    print(f"Erros: {result['errors']}")
```

### Carga Incremental

```python
from etl.incremental import IncrementalLoadManager
import pandas as pd

manager = IncrementalLoadManager('state/state.json')

df = pd.read_csv('data/vendas.csv')
incremental_df = manager.get_incremental_data(
    df=df,
    key_column='id',
    comparison_column='data_venda',
    state_key='vendas_pipeline'
)

print(f"Novos registros: {len(incremental_df)}")
```

### Processamento Paralelo

```python
from etl.parallel import ParallelProcessor
import pandas as pd

def processar_chunk(chunk):
    chunk['total'] = chunk['quantidade'] * chunk['preco']
    return chunk

processor = ParallelProcessor(n_workers=4)
df = pd.read_csv('data/pedidos.csv')

result_df = processor.process_chunks(df, processar_chunk)
```

### Banco de Dados

```python
from etl.database import DatabaseFactory

config = {
    'host': 'localhost',
    'port': 5432,
    'database': 'etl_db',
    'user': 'etl_user',
    'password': 'senha'
}

connector = DatabaseFactory.create('postgres', config)

with connector.transaction():
    df = connector.execute_query("SELECT * FROM vendas WHERE data > '2021-01-01'")
    connector.insert_dataframe(df_processado, 'vendas_processadas')
```

### Checkpoint e Recuperacao

```python
from etl.checkpoint import CheckpointManager, PipelineRecovery

checkpoint_mgr = CheckpointManager('checkpoints/')

checkpoint_id = checkpoint_mgr.create_checkpoint(
    pipeline_name='vendas_pipeline',
    step='transformacao',
    data=df_transformado,
    metadata={'rows': len(df_transformado)}
)

recovery = PipelineRecovery(checkpoint_mgr)
if recovery.can_recover('vendas_pipeline'):
    checkpoint = recovery.recover('vendas_pipeline')
    df = checkpoint['data']
```

### Data Lineage

```python
from etl.lineage import DataLineageTracker

tracker = DataLineageTracker('lineage/lineage.json')

tracker.track_pipeline_execution(
    source='data/vendas.csv',
    transformations=['limpeza', 'agregacao', 'enriquecimento'],
    target='output/vendas_final.parquet',
    metadata={'author': 'pipeline_vendas', 'version': '2.0'}
)

tracker.save()
print(tracker.visualize_graph())
```

### Exportacao Multi-formato

```python
from etl.export import DataExporter
import pandas as pd

df = pd.read_csv('data/relatorio.csv')

exporter = DataExporter()
results = exporter.export_multiple_formats(
    df=df,
    base_path='output/relatorio',
    formats=['csv', 'parquet', 'excel', 'json']
)

for fmt, success in results.items():
    print(f"{fmt}: {'OK' if success else 'FALHOU'}")
```

## CLI

```bash
python main.py input.csv output.parquet \
    --source-type csv \
    --dest-type parquet \
    --remove-duplicates \
    --remove-nulls \
    --validate \
    --metrics
```

## Docker

### Build

```bash
docker-compose build
```

### Executar

```bash
docker-compose up -d
```

### Acessar Adminer

http://localhost:8080

## Testes

### Executar todos os testes

```bash
pytest tests/ -v
```

### Com cobertura

```bash
pytest tests/ -v --cov=etl --cov-report=html
```

### Testes especificos

```bash
pytest tests/test_integration.py -v
```

## Estrutura do Projeto

```
.
├── etl/
│   ├── __init__.py
│   ├── connectors.py          # Conectores de dados
│   ├── database.py            # Conectores DB
│   ├── processors.py          # Processadores
│   ├── orchestrator.py        # Orquestrador
│   ├── pipeline.py            # Pipeline principal
│   ├── incremental.py         # Carga incremental
│   ├── quality.py             # Validacao qualidade
│   ├── schema.py              # Validacao schema
│   ├── parallel.py            # Processamento paralelo
│   ├── lineage.py             # Data lineage
│   ├── checkpoint.py          # Checkpoint/recovery
│   ├── export.py              # Exportacao
│   └── batch_optimizer.py     # Otimizacao batch
├── tests/
│   ├── test_integration.py
│   ├── test_database.py
│   ├── test_quality.py
│   └── test_schema.py
├── main.py
├── requirements.txt
├── docker-compose.yml
├── Dockerfile
├── ARCHITECTURE.md
└── README.md
```

## Tecnologias

- Python 3.8+
- pandas
- pytest
- PostgreSQL
- MySQL
- Docker
- pre-commit

## Licenca

MIT

## Contato

Para duvidas ou sugestoes, abra uma issue.
