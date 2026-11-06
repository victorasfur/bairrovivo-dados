#!/usr/bin/env python3
"""Gera os arquivos de chegada/ SEM o Airflow (plano B, ou para testar rápido).

Faz exatamente o mesmo que as tarefas do DAG: chama a Open-Meteo (real) e gera as ocorrências.
Para o relatório, use o Airflow (os prints do Graph/Grid são exigidos); este script é só um apoio.

  python scripts/gerar_lotes_locais.py --inicio 2026-10-20 --fim 2026-10-31
  python scripts/gerar_lotes_locais.py --inicio 2026-11-01 --fim 2026-11-05 --lat -23.5646 --lon -46.6527
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from bairrovivo import coleta, config  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--inicio", required=True, type=date.fromisoformat)
    p.add_argument("--fim", required=True, type=date.fromisoformat)
    p.add_argument("--lat", type=float, default=config.LAT_PADRAO)
    p.add_argument("--lon", type=float, default=config.LON_PADRAO)
    a = p.parse_args()

    d = a.inicio
    while d <= a.fim:
        ds = d.isoformat()
        coleta.salvar_clima(ds, a.lat, a.lon)
        _, fonte, n = coleta.salvar_ocorrencias(ds, a.lat, a.lon)
        print(f"{ds}: clima ok | ocorrências ({fonte}) = {n}")
        d += timedelta(days=1)
    print(f"Arquivos em {config.CHEGADA_DIR}")


if __name__ == "__main__":
    main()
