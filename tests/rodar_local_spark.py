#!/usr/bin/env python3
"""Teste LOCAL (sem Databricks) da lógica das camadas Prata e Ouro e das 5 consultas SQL.

O que ele faz: lê os arquivos de chegada/, monta "tabelas Bronze" a partir deles (tudo como texto,
como o Auto Loader faria), executa as células dos notebooks 02_prata.py, 03_ouro.py e 04_consultas.sql
e confere o resultado.

O que ele NÃO testa (só existe no Databricks): Auto Loader/checkpoint (01_bronze.py) e o MERGE Delta
(aqui o MERGE é emulado por "remove as chaves antigas + insere o lote"). Nome das tabelas: o prefixo
`workspace.gs` vira `default` e os CREATE OR REPLACE TABLE viram escrita de tabela comum.

  pip install pyspark pandas pyarrow
  python tests/rodar_local_spark.py --chegada chegada      # depois de rodar o DAG/script de lotes
"""
from __future__ import annotations

import argparse
import re
import shutil
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

RAIZ = Path(__file__).resolve().parents[1]
SEP = "\n# COMMAND ----------\n"
SEP_SQL = "\n-- COMMAND ----------\n"


class SparkProxy:
    """Delega ao Spark real, mas traduz o que só existe no Databricks/Delta."""

    def __init__(self, spark):
        self._s = spark

    def __getattr__(self, nome):
        return getattr(self._s, nome)

    def sql(self, q: str):
        m = re.match(r"\s*CREATE OR REPLACE TABLE\s+(\S+)\s+AS\s+(.*)", q, re.S | re.I)
        if m:
            self._s.sql(m.group(2)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(m.group(1))
            return self._s.table(m.group(1))
        m = re.match(r"\s*MERGE INTO\s+(\S+)\s+AS alvo\s+USING\s+(\S+)\s+AS novo\s+ON\s+(.*?)\s+WHEN", q, re.S | re.I)
        if m:
            alvo, novo_nome, cond = m.group(1), m.group(2), m.group(3)
            chaves = re.findall(r"alvo\.(\w+)\s*=\s*novo\.", cond)
            novo = self._s.table(novo_nome)
            if self._s.catalog.tableExists(alvo):
                velho = self._s.table(alvo)
                junto = velho.join(novo.select(*chaves).distinct(), chaves, "left_anti").unionByName(novo)
            else:
                junto = novo
            junto.write.mode("overwrite").saveAsTable("_tmp_merge")
            self._s.table("_tmp_merge").write.mode("overwrite").saveAsTable(alvo)
            return self._s.createDataFrame([], "x int")
        return self._s.sql(q)


def celulas(arquivo: Path, sep: str, destino_vol: str) -> list[str]:
    txt = arquivo.read_text(encoding="utf-8")
    txt = txt.replace("/Volumes/workspace/gs/chegada", destino_vol).replace("workspace.gs", "default")
    return [c.strip("\n") for c in txt.split(sep)[1:] if c.strip()] if sep in txt else [txt]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chegada", default=str(RAIZ / "chegada"))
    ap.add_argument("--lote1-ate", default="", help="(opcional) simula 2 lotes: 1º lote só até esta data AAAA-MM-DD")
    a = ap.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="bv_spark_"))
    (tmp / "exportacao").mkdir()
    spark = (SparkSession.builder.master("local[2]").appName("bv-teste")
             .config("spark.sql.warehouse.dir", str(tmp / "wh"))
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    proxy = SparkProxy(spark)

    def montar_bronze(ate: str = ""):
        for fonte, tabela in [("ocorrencias", "bronze_ocorrencias"), ("clima", "bronze_clima")]:
            arqs = sorted(str(p) for p in (Path(a.chegada) / fonte).glob("*.json") if not ate or p.stem[-10:] <= ate)
            df = (spark.read.option("primitivesAsString", "true").json(arqs)   # tudo texto, como o Auto Loader
                  .withColumn("arquivo_origem", F.col("_metadata.file_path"))
                  .withColumn("carregado_em", F.current_timestamp()))
            df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela)
            print(f"[bronze:{fonte}] {len(arqs)} arquivos, {spark.table(tabela).count()} linhas")

    ns: dict = {
        "spark": proxy,
        "display": lambda df, *x, **k: df.show(20, truncate=False),
        "dbutils": type("D", (), {"fs": type("F", (), {"mkdirs": staticmethod(lambda p: None)})()})(),
    }

    def rodar(nb: str, sep: str = SEP):
        for i, c in enumerate(celulas(RAIZ / "databricks" / nb, sep, str(tmp)), 1):
            try:
                exec(compile(c, f"{nb}#{i}", "exec"), ns)
            except Exception as exc:  # noqa: BLE001
                print(f"\n!!! ERRO em {nb} célula {i}: {type(exc).__name__}: {exc}\n--- código ---\n{c[:900]}")
                raise

    spark.sql("CREATE DATABASE IF NOT EXISTS default")
    if a.lote1_ate:
        montar_bronze(a.lote1_ate)
        print("\n##### PRATA — lote 1 #####")
        rodar("02_prata.py")
        n1 = spark.table("prata_ocorrencias").count()
    montar_bronze()
    print("\n##### PRATA #####")
    rodar("02_prata.py")
    n_oc = spark.table("prata_ocorrencias").count()
    print("\n##### PRATA (2ª execução sem dado novo: não pode mudar a contagem) #####")
    rodar("02_prata.py")
    assert spark.table("prata_ocorrencias").count() == n_oc, "MERGE não foi idempotente"
    if a.lote1_ate:
        print(f"[2 lotes] prata_ocorrencias: {n1} -> {n_oc}")

    print("\n##### OURO #####")
    rodar("03_ouro.py")

    print("\n##### CONSULTAS #####")
    texto = (RAIZ / "databricks" / "04_consultas.sql").read_text(encoding="utf-8").replace("workspace.gs", "default")
    resultados = {}
    for bloco in texto.split(SEP_SQL)[1:]:
        linhas = [l for l in bloco.splitlines() if not l.strip().startswith(("-- MAGIC", "-- DBTITLE"))]
        titulo = next((l for l in bloco.splitlines() if l.startswith("-- DBTITLE")), "").split(",", 1)[-1]
        sql = "\n".join(l for l in linhas if not l.strip().startswith("--")).strip().rstrip(";")
        df = spark.sql(sql)
        print(f"\n>>> {titulo}")
        df.show(10, truncate=False)
        resultados[titulo[:2]] = df.count()
    assert len(resultados) == 5 and all(n > 0 for n in resultados.values()), resultados
    assert (tmp / "exportacao" / "ouro_mvp_ocorrencias.csv").exists()
    print("\nOK: Prata, Ouro e as 5 consultas rodaram.", resultados)
    spark.stop()
    shutil.rmtree(tmp, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
