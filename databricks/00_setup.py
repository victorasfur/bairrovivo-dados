# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup (rodar UMA vez)
# MAGIC Cria o schema `workspace.gs`, o Volume `chegada` e as subpastas onde os lotes do Airflow serão enviados.
# MAGIC
# MAGIC Depois deste notebook, envie os arquivos de `chegada/clima/` e `chegada/ocorrencias/` (gerados pelo Airflow)
# MAGIC para as pastas correspondentes do Volume, pela interface: **Catalog → workspace → gs → chegada → Upload to this volume**.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gs")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.gs.chegada")

BASE = "/Volumes/workspace/gs/chegada"
for sub in ["clima", "ocorrencias", "_controle", "exportacao"]:
    dbutils.fs.mkdirs(f"{BASE}/{sub}")

display(dbutils.fs.ls(BASE))
