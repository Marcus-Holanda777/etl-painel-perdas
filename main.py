from etl_athena_painel.transform import Transform
from etl_athena_painel.load import Load
from pathlib import Path
from dotenv import dotenv_values


BRONZE = Path('bronze')

config = {
    **dotenv_values()
}

def pipe_transform(file: Path) -> Path:
    tr = Transform()
    tbl = tr.transform_file(file)
    origem, destino, file_name = tr.export_file(file, tbl)

    print(f"OK: {file.name} MB( input = {origem:.2f} output = {destino:.2f})")

    return file_name


def pip_load(file: Path, force = False) -> None:

    ld = Load(**config)
    ld.export_table(file, force)

    print(f"LOAD: {file}")
    

if __name__ == '__main__':

    __ = [
        *map(
            pip_load, 
            map(
                pipe_transform, 
                BRONZE.iterdir()
            )
        )
    ]