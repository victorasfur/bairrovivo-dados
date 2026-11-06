# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata (requisito 06)
# MAGIC Tipos corretos, nulos e inválidos tratados, **MERGE pela chave** e **regras de qualidade com o total barrado por cada uma**.
# MAGIC
# MAGIC * `prata_ocorrencias`  — chave `id_ocorrencia` (o mesmo id chega várias vezes: cada mudança de status é uma linha na Bronze; a Prata guarda o estado mais recente).
# MAGIC * `prata_clima`        — chave `local_id + data_hora`.
# MAGIC * `prata_qualidade`    — histórico: regra, linhas avaliadas, linhas reprovadas.
# MAGIC * `prata_*_rejeitadas` — quarentena com o motivo (nada é apagado em silêncio).

# COMMAND ----------

from pyspark.sql import functions as F, Window

S = "workspace.gs"

# Caixa de validação do bairro-piloto (um pouco MAIOR que a usada pelo gerador, 0,008).
# Se trocar o bairro no Airflow, troque aqui também.
LAT0, LON0, RAIO = -23.5646, -46.6527, 0.010
LAT_MIN, LAT_MAX = LAT0 - RAIO, LAT0 + RAIO
LON_MIN, LON_MAX = LON0 - RAIO, LON0 + RAIO

CATEGORIAS = ["Buraco / pavimento", "Iluminação", "Descarte irregular", "Calçada / acessibilidade", "Mobiliário urbano"]
STATUS = ["Recebida", "Em análise", "Encaminhada", "Em atendimento", "Resolvida", "Não procedente"]


def contar(tabela: str) -> int:
    return spark.table(tabela).count() if spark.catalog.tableExists(tabela) else 0


def ok(expr):
    """Regra de qualidade: nulo conta como reprovado."""
    return F.coalesce(expr, F.lit(False))


def avaliar_regras(df, regras: dict, tabela: str):
    """Marca cada linha com os motivos de rejeição e devolve (validas, rejeitadas, resumo por regra)."""
    motivos = F.concat_ws(";", *[F.when(~cond, F.lit(nome)) for nome, cond in regras.items()])
    marcado = df.withColumn("motivos_rejeicao", motivos)   # sem .cache(): o serverless do Databricks Free não suporta
    total = marcado.count()
    somas = marcado.select(*[F.sum((~cond).cast("int")).alias(nome) for nome, cond in regras.items()]).first()
    resumo = [(tabela, nome, total, int(somas[nome] or 0)) for nome in regras]
    validas = marcado.filter(F.col("motivos_rejeicao") == "").drop("motivos_rejeicao")
    rejeitadas = marcado.filter(F.col("motivos_rejeicao") != "")
    return validas, rejeitadas, resumo


def registrar_qualidade(resumo: list) -> None:
    (
        spark.createDataFrame(resumo, "tabela string, regra string, avaliados long, reprovados long")
        .withColumn("executado_em", F.current_timestamp())
        .write.mode("append").saveAsTable(f"{S}.prata_qualidade")
    )


def mostrar_merge(tabela: str) -> None:
    """Métricas do último MERGE (só existem em tabelas Delta)."""
    try:
        h = spark.sql(f"DESCRIBE HISTORY {tabela} LIMIT 1").select("operation", "operationMetrics").first()
        m = h["operationMetrics"]
        print(f"{tabela}: {h['operation']} -> inseridas={m.get('numTargetRowsInserted')} atualizadas={m.get('numTargetRowsUpdated')}")
    except Exception as exc:  # noqa: BLE001
        print("métricas do MERGE indisponíveis:", exc)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ocorrências

# COMMAND ----------

bronze_oc = spark.table(f"{S}.bronze_ocorrencias")
n_bronze_oc = bronze_oc.count()

# try_cast / try_to_timestamp: valor inválido vira NULL (e a regra de qualidade pega), em vez de derrubar o job.
tipado_oc = bronze_oc.select(
    F.trim("id_ocorrencia").alias("id_ocorrencia"),
    F.coalesce(F.trim("titulo"), F.lit("(sem título)")).alias("titulo"),
    F.trim("categoria").alias("categoria"),
    F.trim("status").alias("status"),
    F.trim("prioridade").alias("prioridade"),
    F.coalesce(F.trim("regiao"), F.lit("Não informada")).alias("regiao"),
    F.expr("try_cast(latitude AS DOUBLE)").alias("latitude"),
    F.expr("try_cast(longitude AS DOUBLE)").alias("longitude"),
    F.expr("try_to_timestamp(created_at)").alias("created_at"),
    F.expr("try_to_timestamp(updated_at)").alias("updated_at"),
    F.expr("try_to_timestamp(primeira_analise_at)").alias("primeira_analise_at"),
    F.coalesce(F.trim("user_alias"), F.lit("anonimo")).alias("user_alias"),
    F.col("email"),
    F.coalesce(F.col("origem"), F.lit("desconhecida")).alias("origem"),
    "arquivo_origem",
    "carregado_em",
)

regras_oc = {
    "Q1_chave_obrigatoria": ok(F.col("id_ocorrencia").isNotNull() & (F.col("id_ocorrencia") != "")),
    "Q2_coordenada_no_bairro": ok(F.col("latitude").between(LAT_MIN, LAT_MAX) & F.col("longitude").between(LON_MIN, LON_MAX)),
    "Q3_dominio_categoria_status": ok(F.col("categoria").isin(CATEGORIAS) & F.col("status").isin(STATUS)),
    "Q4_consistencia_temporal": ok(
        F.col("created_at").isNotNull()
        & (F.col("created_at") <= F.current_timestamp())
        & (F.col("updated_at") >= F.col("created_at"))
    ),
}

validas_oc, rejeitadas_oc, resumo_oc = avaliar_regras(tipado_oc, regras_oc, "ocorrencias")

# COMMAND ----------

# Deduplicação dentro do lote: fica a versão mais recente de cada id.
janela = Window.partitionBy("id_ocorrencia").orderBy(F.col("updated_at").desc(), F.col("carregado_em").desc())
lote_oc = (
    validas_oc.withColumn("rn", F.row_number().over(janela))
    .filter("rn = 1").drop("rn", "carregado_em")
)
lote_oc.createOrReplaceTempView("lote_prata_ocorrencias")

n_validas_oc = validas_oc.count()
n_lote_oc = lote_oc.count()

(rejeitadas_oc.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{S}.prata_ocorrencias_rejeitadas"))
registrar_qualidade(resumo_oc)

# COMMAND ----------

# Cria a tabela-alvo vazia na primeira execução, depois MERGE pela chave (idempotente).
if not spark.catalog.tableExists(f"{S}.prata_ocorrencias"):
    lote_oc.limit(0).write.saveAsTable(f"{S}.prata_ocorrencias")

antes_oc = contar(f"{S}.prata_ocorrencias")

spark.sql(f"""
MERGE INTO {S}.prata_ocorrencias AS alvo
USING lote_prata_ocorrencias AS novo
ON alvo.id_ocorrencia = novo.id_ocorrencia
WHEN MATCHED AND novo.updated_at >= alvo.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

depois_oc = contar(f"{S}.prata_ocorrencias")
mostrar_merge(f"{S}.prata_ocorrencias")

# COMMAND ----------

print("=== PRATA · OCORRÊNCIAS (print para o relatório) ===")
print(f"Linhas na Bronze ................................ {n_bronze_oc}")
for _, regra, avaliados, reprovados in resumo_oc:
    print(f"  barradas por {regra:<30} {reprovados}  (de {avaliados})")
print(f"Linhas válidas (passaram nas 4 regras) .......... {n_validas_oc}")
print(f"Versões antigas/duplicatas descartadas no lote ... {n_validas_oc - n_lote_oc}")
print(f"Linhas no lote que vai ao MERGE ................. {n_lote_oc}")
print(f"Linhas na Prata antes / depois do MERGE ......... {antes_oc} / {depois_oc}")

# COMMAND ----------

display(spark.table(f"{S}.prata_qualidade").orderBy(F.col("executado_em").desc(), "tabela", "regra").limit(20))

# COMMAND ----------

display(
    spark.table(f"{S}.prata_ocorrencias_rejeitadas")
    .select("id_ocorrencia", "categoria", "latitude", "created_at", "motivos_rejeicao", "arquivo_origem")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clima (Open-Meteo)
# MAGIC O Open-Meteo devolve **listas paralelas** (`time`, `temperature_2m`, `precipitation`): `arrays_zip` + `explode` transformam em uma linha por hora.

# COMMAND ----------

bronze_cl = spark.table(f"{S}.bronze_clima")
n_bronze_cl = bronze_cl.count()

tipado_cl = (
    bronze_cl.select(
        "latitude", "longitude", "arquivo_origem", "carregado_em",
        F.explode(F.arrays_zip(
            F.col("hourly.time"), F.col("hourly.temperature_2m"), F.col("hourly.precipitation")
        )).alias("z"),
    )
    .select(
        F.format_string("%.2f,%.2f", F.expr("try_cast(latitude AS DOUBLE)"), F.expr("try_cast(longitude AS DOUBLE)")).alias("local_id"),
        F.expr("try_to_timestamp(z.time)").alias("data_hora"),
        F.expr("try_cast(z.temperature_2m AS DOUBLE)").alias("temperatura_c"),
        F.expr("try_cast(z.precipitation AS DOUBLE)").alias("precipitacao_mm"),
        "arquivo_origem", "carregado_em",
    )
)

regras_cl = {
    "Q5_hora_valida": ok(F.col("data_hora").isNotNull()),
    "Q6_precipitacao_valida": ok(F.col("precipitacao_mm") >= 0),
    "Q7_temperatura_plausivel": ok(F.col("temperatura_c").between(-10, 50)),
}
validas_cl, rejeitadas_cl, resumo_cl = avaliar_regras(tipado_cl, regras_cl, "clima")

janela_cl = Window.partitionBy("local_id", "data_hora").orderBy(F.col("carregado_em").desc())
lote_cl = (
    validas_cl.withColumn("rn", F.row_number().over(janela_cl))
    .filter("rn = 1").drop("rn", "carregado_em")
)
lote_cl.createOrReplaceTempView("lote_prata_clima")

(rejeitadas_cl.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{S}.prata_clima_rejeitadas"))
registrar_qualidade(resumo_cl)

# COMMAND ----------

if not spark.catalog.tableExists(f"{S}.prata_clima"):
    lote_cl.limit(0).write.saveAsTable(f"{S}.prata_clima")

antes_cl = contar(f"{S}.prata_clima")

spark.sql(f"""
MERGE INTO {S}.prata_clima AS alvo
USING lote_prata_clima AS novo
ON alvo.local_id = novo.local_id AND alvo.data_hora = novo.data_hora
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

depois_cl = contar(f"{S}.prata_clima")
mostrar_merge(f"{S}.prata_clima")

print("=== PRATA · CLIMA (print para o relatório) ===")
print(f"Documentos na Bronze (1 por arquivo) ............ {n_bronze_cl}")
print(f"Linhas horárias após explode .................... {tipado_cl.count()}")
for _, regra, avaliados, reprovados in resumo_cl:
    print(f"  barradas por {regra:<30} {reprovados}  (de {avaliados})")
print(f"Linhas na Prata antes / depois do MERGE ......... {antes_cl} / {depois_cl}")
