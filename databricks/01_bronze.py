# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze (requisitos 04 e 05)
# MAGIC Carga **incremental e idempotente** com Auto Loader: só arquivos novos entram; rodar de novo não duplica nada.
# MAGIC
# MAGIC **Como gerar a evidência do relatório (item 07):**
# MAGIC 1. Envie o **lote 1** de arquivos ao Volume e rode este notebook → anote as contagens.
# MAGIC 2. Envie o **lote 2** (outros dias de coleta) e rode de novo → só os arquivos novos entram.
# MAGIC 3. Rode uma **3ª vez sem lote novo** → nada muda. Tire print da tabela `controle_ingestao`.

# COMMAND ----------

from pyspark.sql import functions as F

BASE = "/Volumes/workspace/gs/chegada"
SCHEMA = "workspace.gs"


def contar(tabela: str) -> int:
    return spark.table(tabela).count() if spark.catalog.tableExists(tabela) else 0


def ingerir(fonte: str, tabela: str) -> None:
    """Lê de {BASE}/{fonte} apenas o que ainda não foi processado (checkpoint) e anexa na Bronze."""
    antes = contar(tabela)
    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{fonte}")
        .load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(tabela)
    ).awaitTermination()
    depois = contar(tabela)
    arquivos = spark.table(tabela).select("arquivo_origem").distinct().count()

    registro = [(fonte, tabela, antes, depois, depois - antes, arquivos)]
    (
        spark.createDataFrame(
            registro,
            "fonte string, tabela string, linhas_antes long, linhas_depois long, linhas_novas long, arquivos_distintos long",
        )
        .withColumn("executado_em", F.current_timestamp())
        .write.mode("append").saveAsTable(f"{SCHEMA}.controle_ingestao")
    )
    print(f"{tabela}: antes={antes} depois={depois} novas={depois - antes} arquivos_distintos={arquivos}")


# COMMAND ----------

ingerir("clima", f"{SCHEMA}.bronze_clima")

# COMMAND ----------

ingerir("ocorrencias", f"{SCHEMA}.bronze_ocorrencias")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidência: histórico de execuções (print para o relatório)

# COMMAND ----------

display(spark.table(f"{SCHEMA}.controle_ingestao").orderBy("executado_em"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prova de que não duplica: arquivos × linhas
# MAGIC Cada arquivo deve aparecer com a mesma contagem de linhas que tem no arquivo original.

# COMMAND ----------

display(
    spark.table(f"{SCHEMA}.bronze_ocorrencias")
    .groupBy("arquivo_origem").count().orderBy("arquivo_origem")
)
