"""Gerador DETERMINÍSTICO de ocorrências SINTÉTICAS do BairroVivo.

Por que existe: o MVP ainda pode não ter dados reais. Estes dados são FICTÍCIOS,
gerados por regras documentadas aqui, e saem marcados com origem="sintetico".

Regras de geração (documente-as no relatório):
  * Cada dia nasce entre 12 e 22 ocorrências novas, em coordenadas sorteadas
    dentro da caixa do bairro-piloto (config.RAIO_GRAUS).
  * Em dias de chuva REAL (Open-Meteo), o peso da categoria "Buraco / pavimento"
    cresce de 1x a 4x (ver multiplicador_chuva). Essa relação é uma REGRA DO
    GERADOR: serve para a consulta de impacto ter o que mostrar e NÃO é evidência
    real de que chuva causa buracos naquele bairro.
  * Cada ocorrência percorre Recebida -> Em análise -> Encaminhada ->
    Em atendimento -> Resolvida (ou "Não procedente"), com atrasos aleatórios.
    Um arquivo diário traz as ocorrências novas E as atualizações de status do dia
    (por isso o mesmo id aparece em arquivos diferentes: é o que o MERGE resolve).
  * ~9% dos registros saem "sujos" de propósito (coordenada fora do bairro,
    categoria inválida, created_at nulo, título nulo) e ~3% são duplicados exatos,
    para as regras de qualidade da Prata terem o que barrar.

Tudo é função pura de (dia, bairro, chuva, seed): rodar de novo gera o mesmo arquivo.
"""
from __future__ import annotations

import hashlib
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from . import config

CATEGORIAS = {
    # categoria: (prioridade padrão, peso relativo)
    "Buraco / pavimento": ("Alta", 30),
    "Iluminação": ("Média", 25),
    "Descarte irregular": ("Média", 20),
    "Calçada / acessibilidade": ("Alta", 15),
    "Mobiliário urbano": ("Baixa", 10),
}
CATEGORIA_SENSIVEL_A_CHUVA = "Buraco / pavimento"
STATUS_FLUXO = ["Recebida", "Em análise", "Encaminhada", "Em atendimento", "Resolvida"]
STATUS_VALIDOS = STATUS_FLUXO + ["Não procedente"]
LIMIAR_CHUVA_FORTE_MM = 10.0

TITULOS = {
    "Buraco / pavimento": ["Buraco na rua", "Asfalto afundado", "Cratera após chuva"],
    "Iluminação": ["Poste apagado", "Lâmpada queimada", "Rua sem iluminação"],
    "Descarte irregular": ["Entulho na calçada", "Lixo acumulado", "Descarte de sofá"],
    "Calçada / acessibilidade": ["Calçada quebrada", "Rampa obstruída", "Piso tátil danificado"],
    "Mobiliário urbano": ["Banco quebrado", "Placa caída", "Ponto de ônibus danificado"],
}


def _rng(*partes) -> random.Random:
    """Gerador pseudoaleatório estável (não usa hash() do Python, que muda a cada execução)."""
    chave = "|".join(str(p) for p in partes)
    semente = int(hashlib.sha256(chave.encode("utf-8")).hexdigest()[:16], 16)
    return random.Random(semente)


def multiplicador_chuva(mm: float) -> float:
    """1x sem chuva, até 4x com 30 mm ou mais no dia."""
    return 1.0 + min(max(mm or 0.0, 0.0), 30.0) / 10.0


def _setor(lat: float, lon: float, lat0: float, lon0: float) -> str:
    ns = "Norte" if lat >= lat0 else "Sul"
    lo = "Leste" if lon >= lon0 else "Oeste"
    return f"Setor {ns}-{lo}"


def _ciclo_de_vida(r: random.Random, criada: datetime) -> list[tuple[str, datetime]]:
    eventos = [("Recebida", criada)]
    t = criada
    etapas = [
        ("Em análise", (2, 60), 0.92),
        ("Encaminhada", (6, 72), 0.88),
        ("Em atendimento", (12, 120), 0.82),
        ("Resolvida", (24, 240), 0.78),
    ]
    for status, (lo, hi), p_avanca in etapas:
        if r.random() > p_avanca:
            break
        t = (t + timedelta(hours=r.uniform(lo, hi))).replace(microsecond=0)
        eventos.append((status, t))
        if status == "Em análise" and r.random() < 0.08:
            t = (t + timedelta(hours=r.uniform(6, 48))).replace(microsecond=0)
            eventos.append(("Não procedente", t))
            break
    return eventos


def criar_ocorrencias_do_dia(dia: date, lat0: float, lon0: float,
                             chuva_mm: dict[date, float] | None = None,
                             seed: int = config.SEED) -> list[dict]:
    """Ocorrências NASCIDAS em `dia`, com o ciclo de vida completo em `_eventos`."""
    mult = multiplicador_chuva((chuva_mm or {}).get(dia, 0.0))
    n = _rng(seed, "dia", dia.isoformat()).randint(12, 22)
    nomes = list(CATEGORIAS)
    pesos = [CATEGORIAS[c][1] * (mult if c == CATEGORIA_SENSIVEL_A_CHUVA else 1.0) for c in nomes]
    saida = []
    for i in range(1, n + 1):
        oid = f"BV-{dia:%Y%m%d}-{i:03d}"
        r = _rng(seed, "oc", oid)
        categoria = r.choices(nomes, weights=pesos)[0]
        lat = round(lat0 + r.uniform(-config.RAIO_GRAUS, config.RAIO_GRAUS), 6)
        lon = round(lon0 + r.uniform(-config.RAIO_GRAUS, config.RAIO_GRAUS), 6)
        criada = datetime.combine(dia, datetime.min.time()) + timedelta(seconds=r.randint(6 * 3600, 22 * 3600))
        alias = f"morador_{r.randint(1, 150):04d}"
        saida.append({
            "id_ocorrencia": oid,
            "titulo": r.choice(TITULOS[categoria]),
            "categoria": categoria,
            "prioridade": CATEGORIAS[categoria][0],
            "regiao": _setor(lat, lon, lat0, lon0),
            "latitude": lat,
            "longitude": lon,
            "created_at": criada,
            "user_alias": alias,
            "email": f"{alias}@exemplo.invalid",   # .invalid: domínio reservado, e-mail fictício
            "_eventos": _ciclo_de_vida(r, criada),
        })
    return saida


def _registro(oc: dict, idx_evento: int) -> dict:
    status, ts = oc["_eventos"][idx_evento]
    primeira = next((t for s, t in oc["_eventos"][: idx_evento + 1] if s == "Em análise"), None)
    iso = lambda d: d.isoformat(timespec="seconds") if d else None  # noqa: E731
    return {
        "id_ocorrencia": oc["id_ocorrencia"],
        "titulo": oc["titulo"],
        "categoria": oc["categoria"],
        "status": status,
        "prioridade": oc["prioridade"],
        "regiao": oc["regiao"],
        "latitude": oc["latitude"],
        "longitude": oc["longitude"],
        "created_at": iso(oc["created_at"]),
        "updated_at": iso(ts),
        "primeira_analise_at": iso(primeira),
        "user_alias": oc["user_alias"],
        "email": oc["email"],
        "origem": "sintetico",
    }


def eventos_do_dia(dia: date, lat0: float, lon0: float,
                   chuva_mm: dict[date, float] | None = None,
                   seed: int = config.SEED) -> list[dict]:
    """Registros que o 'backend' exportaria em `dia`: novas ocorrências + mudanças de status."""
    registros = []
    d = max(config.INICIO_UNIVERSO, dia - timedelta(days=config.JANELA_HISTORICO_DIAS))
    while d <= dia:
        for oc in criar_ocorrencias_do_dia(d, lat0, lon0, chuva_mm, seed):
            for idx, (_, ts) in enumerate(oc["_eventos"]):
                if ts.date() == dia:
                    registros.append(_registro(oc, idx))
        d += timedelta(days=1)
    return registros


def sujar(registros: list[dict], dia: date, seed: int = config.SEED) -> list[dict]:
    """Injeta problemas de qualidade (determinísticos) para a Prata ter o que barrar."""
    r = _rng(seed, "sujeira", dia.isoformat())
    saida = []
    for rec in registros:
        rec = dict(rec)
        x = r.random()
        if x < 0.03:
            rec["latitude"] = round(rec["latitude"] + 0.5, 6)           # fora do bairro
        elif x < 0.055:
            rec["categoria"] = r.choice(["buraco", "Categoria desconhecida", None])  # fora do domínio
        elif x < 0.075:
            rec["created_at"] = None                                    # timestamp ausente
        elif x < 0.09:
            rec["titulo"] = None                                        # nulo tratável (vira "(sem título)")
        saida.append(rec)
        if r.random() < 0.03:
            saida.append(dict(rec))                                     # duplicata exata
    return saida


def gerar_lote(dia: date, lat0: float, lon0: float,
               chuva_mm: dict[date, float] | None = None,
               seed: int = config.SEED, com_sujeira: bool = True) -> list[dict]:
    registros = eventos_do_dia(dia, lat0, lon0, chuva_mm, seed)
    return sujar(registros, dia, seed) if com_sujeira else registros


def escrever_ndjson(registros: list[dict], destino: Path) -> int:
    """JSON Lines (um objeto por linha): formato que o Auto Loader lê direto."""
    destino = Path(destino)
    destino.parent.mkdir(parents=True, exist_ok=True)
    with destino.open("w", encoding="utf-8") as f:
        for rec in registros:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return len(registros)
