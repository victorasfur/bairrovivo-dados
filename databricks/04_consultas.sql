-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Cinco consultas SQL sobre a Ouro (requisito 07)
-- MAGIC Cada consulta responde uma pergunta do MVP. Para o relatório: **print do resultado + 1 ou 2 frases de interpretação**
-- MAGIC (a nota é pela interpretação, não só pelo SQL). Escreva a interpretação com os SEUS números.
-- MAGIC
-- MAGIC > Atenção ao interpretar a Q4: as ocorrências são **sintéticas** e o gerador foi programado para criar mais relatos de buraco em dias de chuva.
-- MAGIC > A consulta demonstra que o pipeline cruza as duas fontes e produz a métrica; ela **não** prova que chuva causa buracos naquele bairro.

-- COMMAND ----------

-- DBTITLE 1,Q1 · Quantas ocorrências existem por categoria e em que status estão?
SELECT c.categoria,
       f.status,
       COUNT(*) AS ocorrencias
FROM workspace.gs.ouro_fato_ocorrencia f
JOIN workspace.gs.ouro_dim_categoria c ON c.categoria_sk = f.categoria_sk
GROUP BY c.categoria, f.status
ORDER BY c.categoria, ocorrencias DESC;

-- COMMAND ----------

-- DBTITLE 1,Q2 · Quais regiões concentram mais ocorrências e quanto já foi resolvido?
SELECT r.regiao,
       COUNT(*) AS ocorrencias,
       SUM(CASE WHEN f.resolvida THEN 1 ELSE 0 END) AS resolvidas,
       ROUND(100.0 * SUM(CASE WHEN f.resolvida THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_resolvidas
FROM workspace.gs.ouro_fato_ocorrencia f
JOIN workspace.gs.ouro_dim_regiao r ON r.regiao_sk = f.regiao_sk
GROUP BY r.regiao
ORDER BY ocorrencias DESC;

-- COMMAND ----------

-- DBTITLE 1,Q3 · Em quantas horas, na mediana, cada categoria recebe a primeira análise?
SELECT c.categoria,
       COUNT(f.horas_ate_primeira_analise) AS analisadas,
       ROUND(percentile_approx(f.horas_ate_primeira_analise, 0.5), 1) AS mediana_horas,
       ROUND(percentile_approx(f.horas_ate_primeira_analise, 0.9), 1) AS p90_horas
FROM workspace.gs.ouro_fato_ocorrencia f
JOIN workspace.gs.ouro_dim_categoria c ON c.categoria_sk = f.categoria_sk
WHERE f.horas_ate_primeira_analise IS NOT NULL
GROUP BY c.categoria
ORDER BY mediana_horas DESC;

-- COMMAND ----------

-- DBTITLE 1,Q4 (IMPACTO) · Dias de chuva forte geram mais relatos de buraco? (cruza as duas fontes)
WITH dias AS (
  SELECT data_sk, chuva_forte
  FROM workspace.gs.ouro_dim_data
  WHERE chuva_mm IS NOT NULL            -- só dias com clima coletado
),
buracos AS (
  SELECT f.data_criacao_sk, COUNT(*) AS qtd
  FROM workspace.gs.ouro_fato_ocorrencia f
  JOIN workspace.gs.ouro_dim_categoria c ON c.categoria_sk = f.categoria_sk
  WHERE c.categoria = 'Buraco / pavimento'
  GROUP BY f.data_criacao_sk
)
SELECT CASE WHEN d.chuva_forte THEN 'Chuva forte (>= 10 mm)' ELSE 'Sem chuva forte' END AS tipo_de_dia,
       COUNT(*) AS dias_observados,
       SUM(COALESCE(b.qtd, 0)) AS ocorrencias_de_buraco,
       ROUND(AVG(COALESCE(b.qtd, 0)), 2) AS media_de_buracos_por_dia
FROM dias d
LEFT JOIN buracos b ON b.data_criacao_sk = d.data_sk
GROUP BY d.chuva_forte
ORDER BY d.chuva_forte DESC;

-- COMMAND ----------

-- DBTITLE 1,Q5 · Quais locais (≈110 m) têm reincidência da mesma categoria? (prioridade de manutenção)
SELECT f.grid_local,
       c.categoria,
       COUNT(*) AS ocorrencias,
       MIN(f.created_at) AS primeira,
       MAX(f.created_at) AS ultima
FROM workspace.gs.ouro_fato_ocorrencia f
JOIN workspace.gs.ouro_dim_categoria c ON c.categoria_sk = f.categoria_sk
GROUP BY f.grid_local, c.categoria
HAVING COUNT(*) >= 2
ORDER BY ocorrencias DESC, ultima DESC
LIMIT 10;
