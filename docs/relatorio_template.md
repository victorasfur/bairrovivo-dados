# Relatório em PDF · Big Data Architecture & Data Integration

> A ordem abaixo é a exigida no enunciado. Prints ilegíveis, sem legenda ou fora de ordem tiram até 1,0 ponto.
> Preencha, exporte para PDF e coloque o **link do repositório** na capa e na conclusão.

## 01 · Nome completo e RM dos integrantes
| Nome completo | RM |
|---|---|
| | |

_(falta de nome ou RM de qualquer integrante: −0,5)_

## 02 · O produto em uma frase e as perguntas que o MVP responde
**Produto:** BairroVivo é uma plataforma CitizenTech que reúne relatos geolocalizados de problemas urbanos (buracos, iluminação, descarte irregular, calçadas e mobiliário) para que o morador acompanhe o andamento e a gestão priorize onde agir.

**Perguntas que o MVP responde ao cidadão** (ajuste às telas reais do time):
1. Que problemas do meu entorno já foram reportados e em que status estão?
2. Quais regiões e categorias concentram mais relatos?
3. Quanto tempo costuma levar até a primeira análise?
4. Dias de chuva forte aumentam os relatos de buraco?
5. Onde há problemas que se repetem?

## 03 · Diagrama do fluxo de dados, da fonte ao MVP
_Print do diagrama de `docs/arquitetura.md` (o GitHub renderiza o Mermaid) ou redesenhe._
Legenda: …

## 04 · Tabela de fontes
| Fonte | Origem | Formato | Frequência de coleta | Natureza |
|---|---|---|---|---|
| Clima horário | Open-Meteo (API pública, sem chave) | JSON | diária (DAG `@daily`) | real |
| Ocorrências de zeladoria | backend do BairroVivo / gerador `gerador.py` | JSON Lines | diária (DAG `@daily`) | sintética (justificativa: MVP sem usuários reais; ver README) |

## 05 · Requisitos 01 a 09, na ordem, cada um com print e explicação curta
| Req. | O que mostrar (print) | Onde está no repositório |
|---|---|---|
| 01 · fontes | tabela do item 04 + trecho que chama a API | `dags/bairrovivo/coleta.py` |
| 02 · DAG agendado grava em `chegada/` | código do DAG (`schedule="@daily"`) + pasta `chegada/` com arquivos de datas diferentes | `dags/bairrovivo_coleta.py` |
| 03 · paralelismo, conferência, chaves fora do código | visão Graph (2 coletas em paralelo + `conferir_chegada`) + página Admin → Variables | `dags/bairrovivo_coleta.py` |
| 04 · 2 ou mais lotes ao Volume | Volume com os arquivos dos 2 lotes (e as datas de upload) | `databricks/00_setup.py` |
| 05 · Bronze incremental e sem duplicação | tabela `controle_ingestao` + contagem de linhas por arquivo | `databricks/01_bronze.py` |
| 06 · Prata | saída do `02_prata` (barradas por regra) + `prata_qualidade` | `databricks/02_prata.py` |
| 07 · Ouro e 5 consultas | tabelas da Ouro (fato + dimensões) e as consultas (item 08 abaixo) | `databricks/03_ouro.py`, `04_consultas.sql` |
| 08 · LGPD e uso pelo MVP | verificação LGPD + tela do MVP + nome da tabela consumida | `databricks/03_ouro.py` |
| 09 · repositório e README | página do repositório no GitHub | `README.md` |

Explicação curta de cada um (2 a 3 linhas): …

## 06 · Prints do Airflow
* Visão **Graph** com uma execução concluída.
* Visão **Grid** com várias execuções concluídas (datas diferentes).

## 07 · Contagens da Bronze antes e depois do 2º lote e da 2ª execução
| Momento | `bronze_ocorrencias` | `bronze_clima` |
|---|---|---|
| Depois do lote 1 | | |
| Depois do lote 2 (só os arquivos novos entram) | | |
| Depois de rodar de novo sem lote novo (não duplica) | | |

Print de `controle_ingestao`. Legenda: …

## 08 · As 5 consultas SQL, com resultado e interpretação
| # | Pergunta | Print do resultado | Interpretação (com os seus números) |
|---|---|---|---|
| Q1 | volume por categoria e status | | |
| Q2 | regiões com mais ocorrências e % resolvidas | | |
| Q3 | mediana de horas até a primeira análise | | |
| Q4 (impacto) | chuva forte × relatos de buraco | | Lembre: dados sintéticos com efeito de chuva embutido pelo gerador |
| Q5 | locais com reincidência | | |

## 09 · Conclusão técnica e link do repositório
Principais decisões, limitações (ocorrências sintéticas, MERGE/Auto Loader só no Databricks, Prata relê a Bronze inteira) e próximos passos (trocar o gerador pelo backend real, agendar o Databricks).
**Repositório:** https://github.com/SEU-USUARIO/bairrovivo-dados
