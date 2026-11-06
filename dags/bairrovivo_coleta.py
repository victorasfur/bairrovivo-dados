"""DAG de coleta do BairroVivo (Airflow 3).

Requisitos 02 e 03 da disciplina Big Data Architecture & Data Integration:
  * agendado (@daily) e grava UM arquivo por coleta, com a data no nome, em chegada/;
  * 2 tarefas em PARALELO (clima real + ocorrências) e 1 tarefa que CONFERE a chegada;
  * credenciais/parâmetros em Airflow Variables, nunca no código;
  * notificação ao final (log + webhook opcional).

          ┌─ coletar_clima ───────┐
  inicio ─┤                        ├─ conferir_chegada ─ notificar
          └─ coletar_ocorrencias ─┘
"""
from __future__ import annotations

from datetime import timedelta

import pendulum

try:  # Airflow 3
    from airflow.sdk import DAG, Variable
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2.x
    from airflow import DAG
    from airflow.models import Variable
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator

from bairrovivo import coleta, config


def _var(chave: str, padrao: str = "") -> str:
    try:
        return Variable.get(chave, default=padrao)
    except TypeError:  # Airflow 2.x usa default_var
        return Variable.get(chave, default_var=padrao)


def _coord() -> tuple[float, float]:
    return (
        float(_var("bairrovivo_lat", str(config.LAT_PADRAO))),
        float(_var("bairrovivo_lon", str(config.LON_PADRAO))),
    )


def tarefa_clima(ds: str, **_):
    lat, lon = _coord()
    return str(coleta.salvar_clima(ds, lat, lon))


def tarefa_ocorrencias(ds: str, **_):
    lat, lon = _coord()
    destino, fonte, n = coleta.salvar_ocorrencias(
        ds, lat, lon,
        api_url=_var("bairrovivo_api_url", ""),      # vazio ou "mock" => dados sintéticos
        api_token=_var("bairrovivo_api_token", ""),  # token do backend: só em Variable
    )
    return f"{fonte}:{n}"


def tarefa_conferir(ds: str, **_):
    return coleta.conferir_chegada(ds)


def tarefa_notificar(ds: str, resumo: str = "", **_):
    coleta.notificar(ds, resumo, webhook_url=_var("bairrovivo_webhook_url", ""))


with DAG(
    dag_id="bairrovivo_coleta",
    description="Coleta diária: clima (Open-Meteo, real) + ocorrências (backend ou sintético)",
    schedule="@daily",
    # catchup=True cria uma execução para cada dia desde o start_date (backfill).
    start_date=pendulum.datetime(2026, 10, 20, tz="America/Sao_Paulo"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["bairrovivo", "fiap-gs", "bronze"],
    doc_md=__doc__,
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    clima = PythonOperator(
        task_id="coletar_clima",
        python_callable=tarefa_clima,
        op_kwargs={"ds": "{{ ds }}"},
    )
    ocorrencias = PythonOperator(
        task_id="coletar_ocorrencias",
        python_callable=tarefa_ocorrencias,
        op_kwargs={"ds": "{{ ds }}"},
    )
    conferir = PythonOperator(
        task_id="conferir_chegada",
        python_callable=tarefa_conferir,
        op_kwargs={"ds": "{{ ds }}"},
    )
    notificar = PythonOperator(
        task_id="notificar",
        python_callable=tarefa_notificar,
        op_kwargs={"ds": "{{ ds }}", "resumo": "{{ ti.xcom_pull(task_ids='conferir_chegada') }}"},
        trigger_rule="all_success",
    )

    inicio >> [clima, ocorrencias] >> conferir >> notificar
