from athena_mvsh import Athena, CursorParquetDuckdb
from athena_mvsh.dbathena import DBAthena
from pathlib import Path


class Load:
    def __init__(self, **config) -> None:
        self.config = config

    def cursor(self) -> DBAthena:
        """Configura o cursor com as credenciais do AWS

        Returns:
            DBAthena: objeto cursor
        """
        return CursorParquetDuckdb(
            self.config.get("location"),
            result_reuse_enable=True,
            aws_access_key_id=self.config.get("username"),
            aws_secret_access_key=self.config.get("password"),
            region_name=self.config.get("region"),
        )

    def table_exists(self, name: str, schema: str, client: Athena) -> bool:
        """Verifica se tabela existe no Athena

        Args:
            name (str): nome da tabela
            schema (str): schema da tabela
            client (Athena): objeto para executar as consultas

        Returns:
            bool: Verdadeiro ou Falso
        """

        stmt = f"""
        select 1 as col from information_schema.tables 
        where table_name = '{name}' 
        and table_schema = '{schema}'
        """

        rst = client.execute(stmt)
        if rst.fetchone():
            return True

        return False

    def table_optimize(self, name: str, schema: str, client: Athena) -> None:
        """Otimiza a tabela do tipo `ICEBERG`

        Args:
            name (str): nome da tabela
            schema (str): schema da tabela
            client (Athena): objeto para executar as consultas
        """

        optimize = f"OPTIMIZE {schema}.{name} REWRITE DATA USING BIN_PACK"
        vacuum = f"VACUUM {schema}.{name}"

        client.execute(optimize)
        client.execute(vacuum)

    def export_table(self, file: Path, force: bool = False) -> None:
        """_summary_

        Args:
            file (Path): _description_
            force (bool, optional): _description_. Defaults to False.
        """

        name, *__ = file.name.split(".")
        schema = self.config.get("schema_athena")

        tbl_athena = f"painel_{name}"
        with Athena(self.cursor()) as client:
            if_exists = "replace"
            if self.table_exists(tbl_athena, schema, client):
                if_exists = "append"

                if force:
                    if_exists = "replace"

            client.write_table_iceberg(
                file,
                table_name=tbl_athena,
                schema=schema,
                location=f"{self.config.get('location_tables')}tables/{tbl_athena}/",
                if_exists=if_exists,
            )

            if if_exists != "replace":
                self.table_optimize(tbl_athena, schema, client)
