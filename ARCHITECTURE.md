# Arquitetura do Pipeline ETL

## Visão Geral

O pipeline ETL é composto por módulos independentes que trabalham em conjunto para extrair, transformar e carregar dados.

## Componentes

### 1. Extract (Extração)

Responsável por extrair dados de múltiplas fontes:
- Arquivos CSV
- Arquivos JSON
- Arquivos Excel
- Extensível para outras fontes (APIs, bancos de dados)

### 2. Transform (Transformação)

Aplicação de regras de transformação:
- Remoção de duplicatas
- Tratamento de nulos
- Filtragem de dados
- Cálculos e agregações
- Renomeação de colunas

### 3. Load (Carga)

Persistência de dados em diversos formatos:
- CSV
- JSON
- Excel
- Parquet

### 4. Pipeline Orchestrator

Coordena a execução das etapas ETL:
- Execução sequencial Extract -> Transform -> Load
- Rastreamento de execução
- Estatísticas de performance

### 5. Logger

Sistema de logging estruturado:
- Logs rotativos
- Níveis de log configuráveis
- Logs específicos por etapa
- Saída para arquivo e console

### 6. Validators

Sistema de validação de dados:
- Validação de valores nulos
- Validação de unicidade
- Validação de tipos de dados
- Validação de ranges
- Validações customizadas

### 7. Error Handler

Tratamento centralizado de erros:
- Captura e logging de erros
- Exceções customizadas
- Rastreamento de erros
- Decorators para tratamento automático

### 8. Retry Manager

Sistema de retry com exponential backoff:
- Retry configurável
- Estratégias de backoff
- Estatísticas de retry
- Decorators para retry automático

### 9. Metrics Collector

Coleta de métricas de execução:
- Métricas por etapa
- Contadores
- Timers
- Resumos de performance

## Fluxo de Execução

```
[Fonte de Dados]
      ↓
[DataExtractor] → Extrai dados
      ↓
[DataValidator] → Valida dados
      ↓
[DataTransformer] → Transforma dados
      ↓
[DataValidator] → Valida transformações
      ↓
[DataLoader] → Persiste dados
      ↓
[MetricsCollector] → Registra métricas
```

## Tratamento de Erros

Cada módulo possui tratamento de erros específico:
- Erros são logados
- Métricas de erro são coletadas
- Sistema de retry é aplicado quando apropriado
- Exceções customizadas fornecem contexto

## Extensibilidade

O sistema é facilmente extensível:
- Novos extractors podem ser adicionados
- Transformações customizadas via funções
- Novos loaders para diferentes destinos
- Validadores customizados
