#!/usr/bin/env python
"""
CLI para execucao de pipeline ETL
"""

import argparse
import sys
from pathlib import Path

from etl.pipeline import ETLPipeline
from etl.transform import DataTransformer
from etl.logger import get_logger
from etl.validators import DataValidator
from etl.metrics import PipelineMetrics


logger = get_logger('main')


def create_parser():
    """Cria parser de argumentos CLI"""
    parser = argparse.ArgumentParser(
        description='Pipeline ETL para processamento de dados'
    )

    parser.add_argument(
        'source',
        help='Arquivo de origem dos dados'
    )

    parser.add_argument(
        'destination',
        help='Arquivo de destino dos dados'
    )

    parser.add_argument(
        '--source-type',
        default='csv',
        choices=['csv', 'json', 'excel'],
        help='Tipo do arquivo de origem (padrao: csv)'
    )

    parser.add_argument(
        '--dest-type',
        default='csv',
        choices=['csv', 'json', 'excel', 'parquet'],
        help='Tipo do arquivo de destino (padrao: csv)'
    )

    parser.add_argument(
        '--remove-duplicates',
        action='store_true',
        help='Remove linhas duplicadas'
    )

    parser.add_argument(
        '--remove-nulls',
        action='store_true',
        help='Remove linhas com valores nulos'
    )

    parser.add_argument(
        '--validate',
        action='store_true',
        help='Executa validacao de dados'
    )

    parser.add_argument(
        '--metrics',
        action='store_true',
        help='Exibe metricas de execucao'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Modo verbose'
    )

    return parser


def build_transformations(args, transformer):
    """Constroi lista de transformacoes baseado nos argumentos"""
    transformations = []

    if args.remove_duplicates:
        transformations.append(lambda df: transformer.remove_duplicates(df))

    if args.remove_nulls:
        transformations.append(lambda df: transformer.remove_null_rows(df))

    return transformations


def main():
    """Funcao principal"""
    parser = create_parser()
    args = parser.parse_args()

    if not Path(args.source).exists():
        logger.error(f"Arquivo de origem nao encontrado: {args.source}")
        sys.exit(1)

    logger.info(f"Iniciando pipeline ETL")
    logger.info(f"Origem: {args.source} ({args.source_type})")
    logger.info(f"Destino: {args.destination} ({args.dest_type})")

    transformer = DataTransformer()
    transformations = build_transformations(args, transformer)

    if transformations:
        logger.info(f"Transformacoes configuradas: {len(transformations)}")

    pipeline = ETLPipeline(name="CLI Pipeline")
    metrics = PipelineMetrics()

    result = pipeline.run(
        source=args.source,
        destination=args.destination,
        transformations=transformations if transformations else None,
        source_type=args.source_type,
        dest_type=args.dest_type
    )

    if result['status'] == 'success':
        logger.info(f"Pipeline concluido com sucesso!")
        logger.info(f"Linhas processadas: {result['rows_processed']}")
        logger.info(f"Duracao: {result['duration']:.2f}s")

        if args.metrics:
            stats = pipeline.get_execution_stats()
            logger.info(f"Estatisticas: {stats}")

        sys.exit(0)
    else:
        logger.error(f"Pipeline falhou: {result.get('error', 'Erro desconhecido')}")
        sys.exit(1)


if __name__ == '__main__':
    main()
