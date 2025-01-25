from etl_athena_painel.transform import Transform
from etl_athena_painel.load import Load
from pathlib import Path
from dotenv import dotenv_values
from etl_athena_painel.logs import logger
from functools import partial
import argparse
import os
import sys

config = {**dotenv_values()}


def create_medallions():
    lista = ["bronze", "silver"]
    for path in lista:
        try:
            os.mkdir(path)
            logger.info(f"Pasta criada = {path}")
        except FileExistsError:
            logger.warning(f"Pasta [{path}] já existe !")
        except PermissionError:
            logger.error(f"Sem permissão para criar pasta [{path}]")
        except Exception as e:
            logger.error(e)


def pipe_transform(file: Path) -> Path:
    try:
        tr = Transform()
        tbl = tr.transform_file(file)
        origem, destino, file_name = tr.export_file(file, tbl)
        logger.info(f"{file.name} MB( input = {origem:.2f} output = {destino:.2f})")
    except Exception as e:
        logger.error(e)
        sys.exit()
    else:
        return file_name


def pipe_load(file: Path, force=False) -> None:
    try:
        ld = Load(**config)
        ld.export_table(file, force)
        logger.info(f"{file}")
    except Exception as e:
        logger.error(e)
        sys.exit()


def app_cli():
    parser = argparse.ArgumentParser(
        prog="ETL Painel de perdas",
        description="transforma e carrega dados referente ao painel de perdas",
        epilog="Des. Marcus Holanda",
    )

    parser.add_argument("-i", "--infra", action="store_true", help="Cria as camadas")
    parser.add_argument(
        "-f", "--force", action="store_true", help="Deleta as tabelas no Athena"
    )

    try:
        args = parser.parse_args()
    except Exception:
        parser.print_help()
    else:
        logger.info(f"Args: {args.infra=}, {args.force=}")

        if args.infra:
            create_medallions()

        if args.force:
            logger.warning("Deletar tabelas ATHENA")

        bronze = Path("bronze")
        pipe_func = partial(pipe_load, force=args.force)
        __ = [*map(pipe_func, map(pipe_transform, bronze.iterdir()))]
