"""Funções de coleta chamadas pelo DAG. Sem dependência do Airflow, para testar em Python puro."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from . import config, gerador

log = logging.getLogger(__name__)


def _dia(ds: str) -> date:
    return date.fromisoformat(ds)


def caminho_clima(ds: str) -> Path:
    return config.CHEGADA_DIR / config.PASTA_CLIMA / f"clima_{ds}.json"


def caminho_ocorrencias(ds: str) -> Path:
    return config.CHEGADA_DIR / config.PASTA_OCORRENCIAS / f"ocorrencias_{ds}.json"


# --- Fonte 1: Open-Meteo (EXTERNA e REAL) ------------------------------------
def buscar_clima_json(lat: float, lon: float, ds: str, timeout: int = 30) -> dict:
    import requests

    r = requests.get(
        config.OPEN_METEO_URL,
        params={
            "latitude": lat, "longitude": lon,
            "start_date": ds, "end_date": ds,
            "hourly": "temperature_2m,precipitation",
            "timezone": config.TIMEZONE,
        },
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()


def salvar_clima(ds: str, lat: float, lon: float) -> Path:
    dados = buscar_clima_json(lat, lon, ds)
    dados["coleta"] = {
        "fonte": "open-meteo",
        "ds": ds,
        "coletado_em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    destino = caminho_clima(ds)
    destino.parent.mkdir(parents=True, exist_ok=True)
    # uma linha só: o Auto Loader lê JSON Lines
    destino.write_text(json.dumps(dados, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    log.info("clima salvo em %s", destino)
    return destino


def buscar_chuva_diaria(lat: float, lon: float, inicio: date, fim: date, timeout: int = 30) -> dict[date, float]:
    """Chuva diária (mm) real. Se a API falhar devolve {} e o gerador segue sem o efeito da chuva."""
    try:
        import requests

        r = requests.get(
            config.OPEN_METEO_URL,
            params={
                "latitude": lat, "longitude": lon,
                "start_date": inicio.isoformat(), "end_date": fim.isoformat(),
                "daily": "precipitation_sum", "timezone": config.TIMEZONE,
            },
            timeout=timeout,
        )
        r.raise_for_status()
        diario = r.json()["daily"]
        return {
            date.fromisoformat(d): float(mm)
            for d, mm in zip(diario["time"], diario["precipitation_sum"])
            if mm is not None
        }
    except Exception as exc:  # noqa: BLE001 - queremos seguir sem chuva, mas deixar rastro
        log.warning("Não consegui a chuva diária (%s). Gerando ocorrências sem efeito de chuva.", exc)
        return {}


# --- Fonte 2: ocorrências do BairroVivo -------------------------------------
def _baixar_do_backend(api_url: str, api_token: str, ds: str, timeout: int = 30) -> list[dict]:
    import requests

    r = requests.get(
        api_url, params={"data": ds},
        headers={"Authorization": f"Bearer {api_token}"} if api_token else {},
        timeout=timeout,
    )
    r.raise_for_status()
    corpo = r.json()
    return corpo["data"] if isinstance(corpo, dict) else corpo


def salvar_ocorrencias(ds: str, lat: float, lon: float, api_url: str = "", api_token: str = "") -> tuple[Path, str, int]:
    """Usa o backend real se `api_url` estiver definida; senão gera dados SINTÉTICOS."""
    if api_url and api_url.strip().lower() != "mock":
        registros = _baixar_do_backend(api_url, api_token, ds)
        fonte = "backend"
    else:
        dia = _dia(ds)
        inicio = max(config.INICIO_UNIVERSO, dia - timedelta(days=config.JANELA_HISTORICO_DIAS))
        chuva = buscar_chuva_diaria(lat, lon, inicio, dia)
        registros = gerador.gerar_lote(dia, lat, lon, chuva)
        fonte = "sintetico"
    destino = caminho_ocorrencias(ds)
    n = gerador.escrever_ndjson(registros, destino)
    log.info("ocorrências (%s): %s registros em %s", fonte, n, destino)
    return destino, fonte, n


# --- Conferência -------------------------------------------------------------
def conferir_chegada(ds: str) -> dict:
    esperados = {"clima": caminho_clima(ds), "ocorrencias": caminho_ocorrencias(ds)}
    faltando = [nome for nome, p in esperados.items() if not p.exists() or p.stat().st_size == 0]
    if faltando:
        raise FileNotFoundError(f"Arquivos ausentes ou vazios para {ds}: {', '.join(faltando)}")
    resumo = {
        nome: {"arquivo": p.name, "bytes": p.stat().st_size}
        for nome, p in esperados.items()
    }
    resumo["ocorrencias"]["linhas"] = sum(1 for _ in esperados["ocorrencias"].open(encoding="utf-8"))
    log.info("chegada conferida: %s", resumo)
    return resumo


def notificar(ds: str, resumo: str, webhook_url: str = "") -> None:
    msg = f"[BairroVivo] coleta de {ds} concluída: {resumo}"
    log.info(msg)
    if webhook_url:
        try:
            import requests

            requests.post(webhook_url, json={"content": msg}, timeout=10)
        except Exception as exc:  # noqa: BLE001
            log.warning("Webhook falhou (%s). A coleta não é afetada.", exc)
