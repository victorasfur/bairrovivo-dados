# Contrato de dados

Combinado entre quem faz o **backend** (entrada) e quem faz o **MVP/front-end** (saída). Enquanto o backend não existir, o gerador sintético (`dags/bairrovivo/gerador.py`) cumpre o mesmo contrato.

## Entrada: export diário do backend para o Airflow

`GET {bairrovivo_api_url}?data=AAAA-MM-DD` com o cabeçalho `Authorization: Bearer {bairrovivo_api_token}`.
Resposta: lista JSON (ou `{"data": [...]}`) com os registros **que mudaram nesse dia**: ocorrências novas **e** mudanças de status. O mesmo `id_ocorrencia` pode aparecer em dias diferentes.

| Campo | Tipo | Obrigatório | Regra |
|---|---|---|---|
| `id_ocorrencia` | texto | sim | único por ocorrência (ex.: `BV-20261102-001`) |
| `titulo` | texto | não | texto livre (fica só na Prata) |
| `categoria` | texto | sim | `Buraco / pavimento`, `Iluminação`, `Descarte irregular`, `Calçada / acessibilidade`, `Mobiliário urbano` |
| `status` | texto | sim | `Recebida`, `Em análise`, `Encaminhada`, `Em atendimento`, `Resolvida`, `Não procedente` |
| `prioridade` | texto | não | `Alta`, `Média`, `Baixa` |
| `regiao` | texto | não | bairro/setor |
| `latitude`, `longitude` | número | sim | dentro da caixa do bairro-piloto |
| `created_at` | timestamp ISO-8601 (`2026-11-02T14:03:21`) | sim | hora local de São Paulo |
| `updated_at` | timestamp ISO-8601 | sim | `>= created_at`; hora da mudança de status |
| `primeira_analise_at` | timestamp ISO-8601 | não | quando passou para "Em análise" |
| `user_alias` | texto | não | apelido do cidadão (nunca nome real) |
| `email` | texto | não | **não vai para a Ouro** |
| `origem` | texto | recomendado | `backend` ou `sintetico` |

## Saída: o que o MVP consome

Tabela `workspace.gs.ouro_mvp_ocorrencias` (e o CSV `exportacao/ouro_mvp_ocorrencias.csv` no Volume).

| Coluna | Tipo | Uso sugerido na tela |
|---|---|---|
| `id_ocorrencia` | texto | chave |
| `categoria` | texto | filtro e legenda do mapa |
| `regiao` | texto | filtro por região |
| `status` | texto | etiqueta de status |
| `prioridade` | texto | ordenação |
| `latitude`, `longitude` | decimal (≈ 110 m) | marcador no mapa (posição aproximada por LGPD) |
| `created_at`, `updated_at` | timestamp | "registrado em" / "última atualização" |
| `horas_ate_primeira_analise` | decimal | indicador de tempo de resposta |
| `resolvida` | booleano | contador de resolvidas |
| `origem` | texto | selo "dado de demonstração" quando `sintetico` |
