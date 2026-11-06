"""Testes do gerador e da coleta (rodam sem Airflow e sem internet).  pytest -q"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from bairrovivo import config, gerador  # noqa: E402

LAT, LON = config.LAT_PADRAO, config.LON_PADRAO
DIA = date(2026, 11, 2)


def test_deterministico():
    a = gerador.gerar_lote(DIA, LAT, LON)
    b = gerador.gerar_lote(DIA, LAT, LON)
    assert a == b and len(a) > 0


def test_tudo_marcado_como_sintetico():
    assert {r["origem"] for r in gerador.gerar_lote(DIA, LAT, LON)} == {"sintetico"}


def test_lote_limpo_respeita_dominio_e_caixa():
    for r in gerador.gerar_lote(DIA, LAT, LON, com_sujeira=False):
        assert r["categoria"] in gerador.CATEGORIAS
        assert r["status"] in gerador.STATUS_VALIDOS
        assert abs(r["latitude"] - LAT) <= config.RAIO_GRAUS + 1e-9
        assert abs(r["longitude"] - LON) <= config.RAIO_GRAUS + 1e-9
        assert r["created_at"] <= r["updated_at"]


def test_sujeira_existe_para_a_prata_barrar():
    sujo = gerador.gerar_lote(date(2026, 11, 10), LAT, LON)
    limpo = gerador.gerar_lote(date(2026, 11, 10), LAT, LON, com_sujeira=False)
    assert len(sujo) > len(limpo) * 0.95
    assert any(r["created_at"] is None for r in sujo) or any(r["categoria"] not in gerador.CATEGORIAS for r in sujo) \
        or any(abs(r["latitude"] - LAT) > config.RAIO_GRAUS for r in sujo)


def test_mesmo_id_aparece_em_dias_diferentes_com_status_novo():
    ids = {}
    for d in range(1, 15):
        for r in gerador.gerar_lote(date(2026, 11, d), LAT, LON, com_sujeira=False):
            ids.setdefault(r["id_ocorrencia"], set()).add((r["updated_at"], r["status"]))
    assert any(len(v) > 1 for v in ids.values()), "nenhuma ocorrência teve mais de um status"


def test_chuva_aumenta_buracos():
    dia = date(2026, 11, 3)
    seco = gerador.criar_ocorrencias_do_dia(dia, LAT, LON, {dia: 0.0})
    chuva = gerador.criar_ocorrencias_do_dia(dia, LAT, LON, {dia: 30.0})
    pct = lambda lst: sum(o["categoria"] == gerador.CATEGORIA_SENSIVEL_A_CHUVA for o in lst) / len(lst)  # noqa: E731
    assert pct(chuva) >= pct(seco)


def test_ndjson_uma_linha_por_registro(tmp_path):
    regs = gerador.gerar_lote(DIA, LAT, LON)
    destino = tmp_path / "x.json"
    assert gerador.escrever_ndjson(regs, destino) == len(regs)
    linhas = destino.read_text(encoding="utf-8").splitlines()
    assert len(linhas) == len(regs) and all(json.loads(l) is not None for l in linhas)
