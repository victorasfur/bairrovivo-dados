"""Servidor FALSO no formato do Open-Meteo, SÓ PARA TESTES OFFLINE.

Os valores são inventados (determinísticos) e servem apenas para validar o pipeline sem internet.
NUNCA use estes arquivos como a "fonte externa real" do relatório: a fonte real é a API de verdade.

Uso:  python tests/fake_open_meteo.py 8099
      OPEN_METEO_URL=http://127.0.0.1:8099/v1/forecast  (variável de ambiente para o DAG/scripts)
"""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse


def _mm_do_dia(d: date) -> float:
    h = int(hashlib.sha256(d.isoformat().encode()).hexdigest()[:6], 16) % 100
    return 0.0 if h < 55 else round((h - 55) * 0.7, 1)   # ~45% dos dias com chuva, até ~31 mm


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        q = {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}
        ini, fim = date.fromisoformat(q["start_date"]), date.fromisoformat(q["end_date"])
        dias = [ini + timedelta(days=i) for i in range((fim - ini).days + 1)]
        corpo = {"latitude": float(q["latitude"]), "longitude": float(q["longitude"]), "timezone": q.get("timezone")}
        if "hourly" in q:
            horas = [f"{d.isoformat()}T{h:02d}:00" for d in dias for h in range(24)]
            corpo["hourly"] = {
                "time": horas,
                "temperature_2m": [round(18 + 8 * ((int(t[11:13]) - 6) % 24) / 24, 1) for t in horas],
                "precipitation": [round(_mm_do_dia(date.fromisoformat(t[:10])) / 24, 2) for t in horas],
            }
        if "daily" in q:
            corpo["daily"] = {"time": [d.isoformat() for d in dias], "precipitation_sum": [_mm_do_dia(d) for d in dias]}
        dados = json.dumps(corpo).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(dados)

    def log_message(self, *args):  # silencia o log
        pass


if __name__ == "__main__":
    porta = int(sys.argv[1]) if len(sys.argv) > 1 else 8099
    HTTPServer(("127.0.0.1", porta), Handler).serve_forever()
