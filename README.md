# ETL Bases Painel de Perdas

Transformar e carregar, arquivos no formato excel e parquet
para o athena. A exportação sera para tabelas do tipo `ICEBERG`.

## Tecnologias - frameworks

- [ibis-framework](https://ibis-project.org/): biblioteca Python para manipulação e análise de dados que abstrai SQL e integra com diversos backends.
- [athena-mvsh](https://marcus-holanda777.github.io/athena-mvsh/): biblioteca Python personalizada, desenvolvida para facilitar a interação com o Amazon Athena.
- [python-dotenv](https://pypi.org/project/python-dotenv/): biblioteca leve que carrega variáveis de ambiente de arquivos `.env` para o ambiente de execução Python.

## Camadas

O projeto separa os arquivos em três camadas:

- **bronze**: dados brutos, mantendo a origem dos dados exemplo: csv, excel.
- **silver**: arquivos parquet compactados com `ZSTD` e um `ROW_GROUP_SIZE` de 100_000 para desempenho. Nome de colunas normalizadas e também registros de tipo `string`.
- **gold**: tabelas no Athena AWS, formaro `ICEBERG` que possibilita fazer insert delete e update.

### Padrões de nomenclatura para nome de colunas e tabelas

1. Minúsculas por padrão.
2. Sem caracteres especiais e acentuação gráfica.
3. Sem espaços em nomes.
4. Evitar palavras reservadas como (`select`, `vaccum`).
5. Comprimento do nome.

> A etapa de transformação realiza esses ajustes referente ao nome das colunas

```python
from unicodedata import (
    normalize, 
    combining
)
import re

def rename_cols(col: str) -> str:
    col = col.strip()
    col = re.sub(r" +", " ", col)
    col = re.sub(r"\((R\$|%)\)", "", col, re.I)
    col = normalize(
        "NFC", "".join(c for c in normalize("NFD", col) if not combining(c))
    )

    return "_".join(col.strip().lower().split())
```

## Arquitetura do Processo
O pipeline de processamento de dados é dividido em duas etapas principais:

### 1. Transformação e Normalização
- Leitura de arquivos de entrada no formato **Excel (.xlsx)** e **Parquet (.parquet)**.
- Aplicação de regras de normalização e otimização dos dados.
- Escrita dos dados processados em **Parquet otimizado**.

### 2. Exportação para Iceberg no Athena
- Leitura dos arquivos Parquet otimizados gerados na etapa anterior.
- Criação e atualização de tabelas no formato **Apache Iceberg** no **AWS Athena**.
- Garantia de integridade e particionamento eficiente dos dados.