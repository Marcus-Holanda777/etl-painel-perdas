import ibis
from ibis import selectors as s, _
from ibis.expr import types as ir
from pathlib import Path
import pandas as pd
from unicodedata import normalize, combining
import re


"""
 - FUNCOES INTERNAS DO DUCKDB QUE NAO EXISTE NO IBIS
"""


@ibis.udf.scalar.builtin
def strip_accents(string: str) -> str:
    """Retira acentos de `string` (func duckdb)"""
    ...


class Transform:
    def __init__(self, database: str = "transform.duckdb") -> None:
        self.database = database
        self.con = ibis.connect(f"duckdb://{self.database}")

    def rename_cols(self, col: str) -> str:
        col = col.strip()
        col = re.sub(r" +", " ", col)
        col = re.sub(r"\((R\$|%)\)", "", col, re.I)
        col = normalize(
            "NFC", "".join(c for c in normalize("NFD", col) if not combining(c))
        )

        return "_".join(col.strip().lower().split())

    def clear_tbl(self, tbl: ir.Table) -> ir.Table:
        return tbl.mutate(
            s.across(
                s.of_type("string") | s.of_type("!string"),
                strip_accents(_.strip().upper()),
            )
        )

    def transform_file(self, file: Path, table_name: str = "mutate") -> ir.Table:
        if file.suffix == ".xlsx":
            table = self.con.create_table(
                table_name, obj=pd.read_excel(file), temp=True, overwrite=True
            )
        else:
            table = self.con.read_parquet(file)

        return table.rename(self.rename_cols).pipe(self.clear_tbl)

    def export_file(
        self, file: Path, tbl: ir.Table, camada: str = "silver"
    ) -> tuple[float, float, Path]:
        MB = 1 << 20

        origem_size = file.stat().st_size / MB

        file_name = Path(
            f"{camada}/{self.rename_cols(file.name.split('.')[0])}.parquet"
        )

        tbl.to_parquet(file_name, compression="zstd", row_group_size=100_000)
        destino_size = file_name.stat().st_size / MB

        return origem_size, destino_size, file_name
