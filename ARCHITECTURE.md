# Arquitetura do Sistema ETL

## Visao Geral

Sistema ETL modular e extensivel para processamento de dados em larga escala.

## Componentes Principais

### 1. Conectores (etl/connectors.py)

Responsavel por abstrair diferentes fontes de dados.

**Classes:**
- `BaseConnector`: Interface base para conectores
- `FileConnector`: Conectores para arquivos (CSV, JSON, Parquet, Excel)
- `ConnectorFactory`: Factory pattern para criacao de conectores

**Fluxo:**
```
Fonte de Dados -> Conector -> DataFrame
```

### 2. Processadores (etl/processors.py)

Processamento e transformacao de dados.

**Classes:**
- `BaseProcessor`: Interface base
- `CleaningProcessor`: Limpeza de dados (duplicatas, nulos)
- `TransformationProcessor`: Transformacoes customizadas
- `ProcessorChain`: Encadeamento de processadores

**Padrao Chain of Responsibility:**
```
DataFrame -> Processor1 -> Processor2 -> ... -> DataFrame processado
```

### 3. Orquestrador (etl/orchestrator.py)

Coordena execucao do pipeline completo.

**Responsabilidades:**
- Gerenciar fluxo Extract-Transform-Load
- Coordenar processadores
- Controlar transacoes
- Logging e metricas

### 4. Banco de Dados (etl/database.py)

Conectores para bancos relacionais.

**Suporte:**
- PostgreSQL (psycopg2)
- MySQL (pymysql)
- Context managers para transacoes

### 5. Carga Incremental (etl/incremental.py)

Gerenciamento de cargas incrementais e CDC.

**Features:**
- Rastreamento de estado
- Deteccao de mudancas (inserts, updates, deletes)
- Persistencia de checkpoints

### 6. Qualidade de Dados (etl/quality.py)

Validacao e garantia de qualidade.

**Validacoes:**
- Completude
- Unicidade
- Range de valores
- Padroes (regex)
- Checks customizados

### 7. Schema (etl/schema.py)

Validacao e inferencia de schemas.

**Capacidades:**
- Definicao de schemas tipados
- Validacao de estrutura
- Inferencia automatica
- Conversao de/para JSON

### 8. Processamento Paralelo (etl/parallel.py)

Processamento otimizado multi-core.

**Estrategias:**
- Divisao em chunks
- ProcessPoolExecutor
- ThreadPoolExecutor
- Batch processing

### 9. Data Lineage (etl/lineage.py)

Rastreamento de linhagem de dados.

**Recursos:**
- Tracking de origem e destino
- Registro de transformacoes
- Exportacao para JSON
- Visualizacao de grafo

### 10. Checkpoint e Recuperacao (etl/checkpoint.py)

Sistema de recuperacao de falhas.

**Funcionalidades:**
- Salvamento de estado intermediario
- Recuperacao de execucao
- Cleanup de checkpoints antigos
- Decorators para steps

### 11. Export (etl/export.py)

Exportacao para multiplos formatos.

**Formatos:**
- CSV (com compressao)
- Parquet (com particoes)
- JSON
- Excel
- HTML
- Markdown

### 12. Otimizacao de Batch (etl/batch_optimizer.py)

Otimizacoes avancadas de processamento.

**Tecnicas:**
- Batch size adaptativo
- Memoria eficiente
- Streaming processing
- Paralelizacao de batches

## Fluxo de Dados

```
┌─────────────┐
│   Fonte     │
└──────┬──────┘
       │
       v
┌─────────────┐
│  Conector   │
└──────┬──────┘
       │
       v
┌─────────────┐
│  Extract    │
└──────┬──────┘
       │
       v
┌─────────────┐
│ Processador │
└──────┬──────┘
       │
       v
┌─────────────┐
│  Validacao  │
└──────┬──────┘
       │
       v
┌─────────────┐
│    Load     │
└──────┬──────┘
       │
       v
┌─────────────┐
│   Destino   │
└─────────────┘
```

## Padroes de Design

### Factory Pattern
Usado em `ConnectorFactory` e `DatabaseFactory` para criacao de objetos.

### Chain of Responsibility
Usado em `ProcessorChain` para encadear processadores.

### Strategy Pattern
Usado em processadores para diferentes estrategias de transformacao.

### Decorator Pattern
Usado em `RecoverableStep` para adicionar checkpoint.

### Observer Pattern
Callbacks de progresso em batch processors.

## Extensibilidade

### Adicionar Novo Conector

```python
class CustomConnector(BaseConnector):
    def read(self, source, **kwargs):
        # Implementacao
        pass

    def write(self, df, destination, **kwargs):
        # Implementacao
        pass
```

### Adicionar Novo Processador

```python
class CustomProcessor(BaseProcessor):
    def process(self, df):
        # Implementacao
        return df
```

### Adicionar Nova Validacao

```python
validator = DataQualityValidator()
validator.add_custom_check(
    name='custom_check',
    check_func=lambda df: my_validation(df),
    description='Minha validacao'
)
```

## Boas Praticas

1. **Idempotencia**: Pipelines devem ser rexecutaveis sem efeitos colaterais
2. **Checkpoints**: Usar checkpoints em pipelines longos
3. **Validacao**: Sempre validar dados antes de carregar
4. **Logging**: Usar logger estruturado para rastreabilidade
5. **Testes**: Manter cobertura > 80%

## Metricas e Monitoramento

- Tempo de execucao por etapa
- Numero de registros processados
- Taxa de sucesso/falha
- Memoria utilizada
- Data lineage completo

## Escalabilidade

- Processamento paralelo para grandes volumes
- Batch adaptativo para otimizacao
- Streaming para dados continuos
- Particoes para distribuicao

## Seguranca

- Credenciais em variaveis de ambiente
- Transacoes para atomicidade
- Validacao de schema
- Logs sanitizados

---

*Arquitetura projetada para escalabilidade, manutenibilidade e extensibilidade.*
