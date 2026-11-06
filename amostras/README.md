# Amostras

A disciplina pede "um pouco de cada fonte" aqui. Depois de rodar o DAG, copie 2 arquivos de cada pasta de `chegada/`:

```bash
cp chegada/clima/clima_2026-11-01.json       amostras/clima/
cp chegada/clima/clima_2026-11-02.json       amostras/clima/
cp chegada/ocorrencias/ocorrencias_2026-11-01.json amostras/ocorrencias/
cp chegada/ocorrencias/ocorrencias_2026-11-02.json amostras/ocorrencias/
```

* `clima/`: dados **reais** do Open-Meteo. Esta pasta fica vazia até você rodar o DAG com internet de verdade (não incluímos arquivo inventado aqui para não passar dado falso por real).
* `ocorrencias/`: `ocorrencias_exemplo_formato.json` mostra só o **formato** (10 linhas sintéticas). Troque pelos arquivos do seu DAG.

Não coloque dados pessoais reais nem chaves nesta pasta.
