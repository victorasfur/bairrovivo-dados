"""Parâmetros do pipeline BairroVivo.

Aqui ficam só valores NÃO sensíveis. Chaves, tokens e URLs que mudam por ambiente
vêm de Airflow Variables (veja README, passo 5).
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

# --- Bairro-piloto -----------------------------------------------------------
# Valores padrão. Podem ser trocados sem mexer no código pelas Variables do
# Airflow `bairrovivo_lat` e `bairrovivo_lon`.
NOME_BAIRRO = "Bela Vista - São Paulo/SP"
LAT_PADRAO = -23.5646
LON_PADRAO = -46.6527
# Meia-largura da "caixa" do bairro, em graus (0,008 ≈ 900 m). O gerador sorteia
# coordenadas dentro dela e a regra de qualidade da Prata valida contra ela.
RAIO_GRAUS = 0.008

# --- Fonte externa real: Open-Meteo -----------------------------------------
# A API "forecast" aceita start_date/end_date para os últimos ~90 dias.
OPEN_METEO_URL = os.environ.get("OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast")
TIMEZONE = "America/Sao_Paulo"

# --- Dados sintéticos (ocorrências do BairroVivo) ----------------------------
SEED = 42                           # mesma semente => mesmos dados (reprodutível)
INICIO_UNIVERSO = date(2026, 10, 20)  # primeiro dia com ocorrências
JANELA_HISTORICO_DIAS = 45          # quanto do passado entra no cálculo do ciclo de vida

# --- Pasta de pouso ("chegada/") ---------------------------------------------
# Dentro do Docker é /opt/airflow/chegada (definido no docker-compose.override.yml).
CHEGADA_DIR = Path(
    os.environ.get("CHEGADA_DIR", Path(__file__).resolve().parents[2] / "chegada")
)
PASTA_CLIMA = "clima"
PASTA_OCORRENCIAS = "ocorrencias"
