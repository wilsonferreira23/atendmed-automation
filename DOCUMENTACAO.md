# Documentação Técnica — Automação TENEX → MEDICAR

> **Para quem está lendo isto:** Esta documentação foi escrita para que qualquer IA ou desenvolvedor consiga entender 100% o sistema sem contexto prévio. Leia do início ao fim antes de modificar qualquer arquivo.

---

## 1. O que este sistema faz

Este sistema é uma **integração automática** entre duas plataformas:

- **TENEX** — Sistema de gestão da clínica (ERP). Onde os clientes são cadastrados, os contratos são gerenciados e os dependentes são registrados.
- **MEDICAR** — Operadora/plataforma de plano de saúde (sistema TOTVS Protheus). Onde os beneficiários precisam ser formalmente incluídos ou excluídos do plano.

O problema que ele resolve: toda vez que um cliente é cadastrado, atualizado ou cancelado no Tenex, os mesmos dados precisam ser refletidos na Medicar — manualmente isso seria trabalhoso e sujeito a erros. A automação faz isso **automaticamente via webhooks** e também disponibiliza um **painel web** para operações manuais quando necessário.

### Fluxo geral

```
[TENEX cadastra cliente]
        │
        ▼ (webhook HTTP POST)
[Nossa API FastAPI]
        │
        ├── Busca dados completos no TENEX (carteira virtual, dependentes)
        ├── Insere/cancela beneficiário na MEDICAR via API TOTVS
        └── Registra resultado no PostgreSQL (histórico do painel)
```

---

## 2. Estrutura de arquivos

```
convenio_automacao/
├── app.py              ← Toda a lógica do servidor (FastAPI + regras de negócio)
├── static/
│   └── index.html      ← Painel web administrativo (HTML + JS puro, sem frameworks)
├── requirements.txt    ← Dependências Python
├── Dockerfile          ← Imagem Docker para deploy no Railway
└── tenex doc.txt       ← Documentação da API do Tenex (referência)
```

Os arquivos `*.py` na raiz (ex: `test_create2.py`, `audit_hardcoded.py`) são **scripts utilitários de desenvolvimento**, não fazem parte da aplicação em produção.

---

## 3. Infrastructure e Deploy

- **Plataforma:** Railway (https://railway.app)
- **Repositório:** GitHub (`wilsonferreira23/atendmed-automation`, branch `main`)
- **Deploy:** Automático a cada push no `main` via integração Railway + GitHub
- **Banco de dados:** PostgreSQL gerenciado pelo Railway (serviço separado, conectado via `DATABASE_URL`)
- **Porta:** 8000 (definida no Dockerfile)
- **URL pública:** Gerada automaticamente pelo Railway

### Por que PostgreSQL e não SQLite?

O sistema foi originalmente desenvolvido com SQLite, mas o Railway usa containers efêmeros: a cada novo deploy o container é recriado do zero e qualquer arquivo local (como o `.db` do SQLite) é **perdido**. Por isso migramos para PostgreSQL, que é um serviço externo e persiste os dados independentemente dos deploys.

---

## 4. Variáveis de Ambiente

**Todas** as configurações sensíveis ou específicas de cada clínica ficam em variáveis de ambiente, configuradas no painel do Railway (Settings > Variables). Isso permite usar o **mesmo repositório GitHub** para múltiplas clínicas — cada instância do Railway tem seu próprio conjunto de variáveis.

### Variáveis obrigatórias

| Variável | Descrição |
|---|---|
| `DATABASE_URL` | URL de conexão PostgreSQL. Formato: `postgresql://user:pass@host:5432/dbname`. No Railway, é gerada automaticamente ao linkar o serviço de banco. |
| `TENEX_BASE_URL` | URL base da clínica no Tenex. **Sem valor padrão** — se não configurada, a integração com o Tenex não funciona. Ex: `https://maisaudebh.tenex.com.br` |
| `TENEX_BASIC_AUTH` | Token de autenticação do Tenex em formato Base64. Obtido no painel admin do Tenex. Usado no header `Authorization: Basic {token}`. |
| `MEDICAR_BASE_URL` | URL base da API da Medicar. Ex: `https://totvs.medicar.com.br` |
| `MEDICAR_USERNAME` | Usuário da conta Medicar para obter token OAuth2. |
| `MEDICAR_PASSWORD` | Senha da conta Medicar. |
| `MEDICAR_CNPJMEDICAR` | CNPJ da operadora Medicar (só dígitos). Usado para identificar qual contrato buscar. |
| `MEDICAR_GRUPOEMPRESA` | Código do grupo de empresa na Medicar. |
| `MEDICAR_CONTRATO` | Código do contrato na Medicar. |
| `TENANT_ID` | ID do tenant na Medicar (multi-empresa). Obrigatório para cancelamentos. |

### Variáveis opcionais (têm padrão)

| Variável | Padrão | Descrição |
|---|---|---|
| `PAINEL_SENHA` | `med123` | Senha de acesso ao painel web. **Mude isso em produção!** Cada clínica deve ter sua própria senha. |
| `MEDICAR_CONTRACT_FIELDS_JSON` | `""` (vazio) | JSON com os campos do cabeçalho MASTERBBA para a API da Medicar. Se vazio, usa os valores padrão hardcoded. Ver seção 7 para detalhes. |
| `PLAN_MAPPING_JSON` | `{"31":{"codpro":"0066","versao":"001"},...}` | Mapeamento de `id_plano` do Tenex para o código de plano da Medicar. A chave é o `id_plano` da carteira virtual do Tenex. |
| `MEDICAR_PLANO_PADRAO_JSON` | `{"codpro": "0066", "versao": "001"}` | Plano usado nas inclusões manuais (quando não há plano vindo do Tenex). |

---

## 5. Banco de Dados — Tabelas

O sistema cria e gerencia duas tabelas automaticamente na inicialização (`init_db()`):

### `historico_operacoes`

Registro de **todas** as inclusões e exclusões processadas. Exibido no painel web na aba "Histórico".

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | SERIAL PK | ID auto-incremental |
| `tipo` | TEXT | `"inclusao"` ou `"exclusao"` |
| `status` | TEXT | `"processando"`, `"sucesso"`, `"erro"`, `"ignorado"` |
| `cpf` | TEXT | CPF do titular (somente dígitos) |
| `nome` | TEXT | Nome do titular |
| `id_plano` | TEXT | Código do plano na Medicar |
| `dependentes` | TEXT | JSON com lista de dependentes incluídos |
| `mensagem` | TEXT | Mensagem de resultado ou erro |
| `data_hora` | TEXT | Timestamp UTC no formato ISO 8601 |
| `origem` | TEXT | `"webhook"`, `"PAINEL WEB"`, etc. |

**Padrão de uso:** Quando uma operação começa, ela é inserida com `status="processando"`. Quando termina (com sucesso ou erro), o registro é atualizado via `db_atualizar_operacao()`. Isso garante que o usuário veja a operação no painel imediatamente, mesmo enquanto ela ainda está sendo processada.

### `clientes_excluidos`

Registro de clientes que foram **cancelados** no Tenex, para evitar reprocessamento duplicado quando o webhook de dependentes chegar depois do webhook de cancelamento.

| Coluna | Tipo | Descrição |
|---|---|---|
| `id_cliente` | BIGINT PK | ID interno do cliente no Tenex |
| `cpf` | TEXT | CPF do cliente |
| `data_exclusao` | TEXT | Data/hora UTC do cancelamento |

---

## 6. Fluxos de Negócio Detalhados

### 6.1 Fluxo de Novo Cliente (Inclusão Automática)

**Trigger:** Tenex envia um POST para `/webhook/novo-cliente` com `operation: "insert"`.

```
1. POST /webhook/novo-cliente recebido
2. Para cada item recebido:
   a. Registra operação como "processando" no banco
   b. Lança process_novo_cliente_bg() em background (não bloqueia o webhook)
   c. Retorna 200 OK imediatamente para o Tenex

3. [Em background] process_novo_cliente_bg():
   a. Obtém token Medicar (com cache)
   b. Chama process_novo_cliente_item()
   c. Atualiza o registro no banco com o resultado

4. [Em background] process_novo_cliente_item():
   a. Tenta buscar carteira virtual do cliente no Tenex (até 10 vezes, 60s entre tentativas)
      MOTIVO DO RETRY: o plano pode demorar para aparecer no Tenex após o cadastro
   b. Extrai o id_plano da carteira e mapeia para o código Medicar via PLAN_MAPPING_JSON
   c. Busca os dados completos do cliente + contatos/dependentes no Tenex
   d. Chama medicar_incluir_titular() para incluir titular E dependentes em uma única chamada
   e. Retorna dict com status e dados
```

**Por que Background Tasks?** O Tenex tem um timeout curto para o webhook. O processo pode demorar até 10 minutos (10 tentativas × 60s). Se a API bloqueasse esperando, o Tenex consideraria o webhook como falho e tentaria reenviar. Com Background Tasks, a resposta é imediata e o processamento continua em paralelo.

### 6.2 Fluxo de Atualização de Dependentes

**Trigger:** Tenex envia POST para `/webhook/dependentes` com `operation: "update"`.

```
1. POST /webhook/dependentes recebido
2. Para cada item com operation == "update":
   a. Verifica se o cliente está na tabela clientes_excluidos
      → Se estiver: ignora (cliente foi cancelado, não deve reincluir)
   b. Obtém token Medicar
   c. Busca dados completos + contatos do cliente no Tenex
   d. Separa titular (principal=1) e dependentes (principal=0)
   e. Verifica se o cliente já existe na Medicar buscando pelo CPF
   f. Se já existe: inclui apenas os dependentes novos
   g. Se não existe: inclui titular + dependentes
   h. Registra no banco
```

**Por que verifica clientes_excluidos?** Quando um cliente é cancelado, às vezes o webhook de "update" chega depois. Sem essa verificação, o sistema reincluiria indevidamente um cliente cancelado na Medicar.

### 6.3 Fluxo de Cancelamento (Exclusão Manual via Painel)

```
1. Operador digita CPF no painel → POST /cancelar-por-cpf
2. API busca matrícula (BBA_MATRIC) do CPF na Medicar
   → Se não tem matrícula: retorna "não encontrado"
3. Chama endpoint blockProtocol da Medicar para cancelar
4. Registra operação de exclusão no banco
5. Salva cliente na tabela clientes_excluidos
```

### 6.4 Fluxo de Inclusão Manual via CPF do Tenex

**Novo fluxo** (adicionado para corrigir clientes que falharam na automação):

```
1. Operador digita CPF no painel → POST /incluir-por-cpf
2. API busca o cliente na carteira virtual do Tenex pelo CPF
   → Fallback: busca na API de clientes se a carteira não retornar
3. Obtém o id_cliente do Tenex
4. Registra operação como "processando" no banco
5. Lança process_novo_cliente_bg() em background (mesmo fluxo do webhook)
6. Retorna resposta imediata ao operador
```

---

## 7. API da Medicar — Detalhes Técnicos

A Medicar usa o framework **TOTVS Protheus** com modelo `PLIncBenModel`. A inclusão de beneficiários é feita via um payload JSON complexo.

### Autenticação

OAuth2 Resource Owner Password Grant. O token expira e é cacheado em `_token_cache` para evitar nova autenticação em cada chamada.

```
POST /api/oauth2/v1/token?grant_type=password&username=...&password=...
Resposta: {"access_token": "...", "expires_in": 3600}
```

### Campos MASTERBBA (cabeçalho da inclusão)

Estes são os identificadores do contrato da clínica na plataforma Medicar. Eles **precisam ser fornecidos** a cada chamada de inclusão:

| Campo | Significado |
|---|---|
| `BBA_CODINT` | Código da interface/operadora |
| `BBA_CODEMP` | Código da empresa na Medicar |
| `BBA_CONEMP` | Número do contrato |
| `BBA_VERCON` | Versão do contrato |
| `BBA_SUBCON` | Número do sub-contrato |
| `BBA_VERSUB` | Versão do sub-contrato |

**Como configurar:** Preencha a variável `MEDICAR_CONTRACT_FIELDS_JSON` com um JSON contendo esses 6 campos. Se a variável estiver vazia, o sistema usa os valores hardcoded no `master_defaults` dentro de `medicar_incluir_titular()` — esses valores são específicos da AtendMed BH e estarão **errados** para outras clínicas.

### Campos B2N (dados do beneficiário)

| Campo | Titular | Dependente | Significado |
|---|---|---|---|
| `B2N_NOMUSR` | Nome ASCII maiúsc. | Idem | Nome completo |
| `B2N_DATNAS` | YYYYMMDD | Idem | Data de nascimento |
| `B2N_GRAUPA` | `"00"` | `"11"` | Grau de parentesco |
| `B2N_ESTCIV` | `"S"` | `"S"` | Estado civil (sempre Solteiro) |
| `B2N_SEXO` | `"1"` masc / `"2"` fem | Idem | Sexo |
| `B2N_CPFUSR` | Só dígitos | Idem | CPF |
| `B2N_MAE` | ASCII maiúsc. | Idem | Nome da mãe |

**Atenção ao encoding:** A Medicar rejeita caracteres especiais (acentos, ç, etc.) em nomes. Por isso a função `only_ascii_upper()` é chamada em todos os nomes antes de enviar.

### Cancelamento de matrícula

Primeiro busca-se o `BBA_MATRIC` (número de matrícula) via `GET /client/v1/contract?cgcbeneficiario={cpf}&...`, depois:

```
POST /totvsHealthPlans/familyContract/v1/beneficiaries/blockProtocol
Body: {
  "subscriberId": "...",   ← BBA_MATRIC obtido antes
  "reason": "000001",      ← código de motivo
  "blockDate": "YYYY-MM-DD",
  "loginUser": "..."
}
```

---

## 8. API do Tenex

Acessada com autenticação Basic (Base64 do token em `TENEX_BASIC_AUTH`).

### Endpoints utilizados

**Carteira Virtual:**
```
GET /api/v2/carteira-virtual/{cpf_somente_digitos}
Resposta: lista de pessoas com seus planos_contratados
```
Usado para verificar se o cliente tem um plano ativo e qual é o `id_plano`.

**Cliente com Contatos (Dependentes):**
```
GET /api/v2/clientes/?id={id_cliente}&_expand=contatos
Resposta: dados completos do cliente incluindo seus contatos
```
O campo `principal=1` identifica o titular; `principal=0` identifica dependentes. Dependentes sem CPF são ignorados.

---

## 9. Endpoints da API (app.py)

### Webhooks (chamados pelo Tenex automaticamente)

| Método | Rota | Quando é chamado |
|---|---|---|
| POST | `/webhook/novo-cliente` | Tenex cadastra um novo cliente (operation: insert) |
| POST | `/webhook/dependentes` | Tenex atualiza dados de um cliente (operation: update) |

### Painel web (chamados pelo frontend)

| Método | Rota | Descrição |
|---|---|---|
| POST | `/cancelar-por-cpf` | Cancela um CPF na Medicar |
| POST | `/cancelar-em-lote` | Cancela vários CPFs em paralelo |
| POST | `/incluir-manual` | Inclui titular + dependentes informados manualmente |
| POST | `/incluir-manual-lote` | Inclusão em lote via CSV |
| POST | `/incluir-por-cpf` | Inclui cliente buscando dados no Tenex pelo CPF |
| GET | `/api/historico` | Lista histórico de operações com paginação e filtros |
| GET | `/api/config` | Retorna configurações para o frontend (ex: senha do painel) |

### Diagnóstico

| Método | Rota | Descrição |
|---|---|---|
| GET | `/health` | Health check simples |
| GET | `/debug/db` | Verifica conexão com PostgreSQL e conta registros |

---

## 10. Painel Web (index.html)

SPA simples em HTML + JavaScript puro (sem React, Vue, etc.).

### Autenticação
- A senha é carregada do backend via `GET /api/config` ao iniciar a página
- Não é autenticação real em servidor — é apenas proteção de tela no cliente
- A senha é configurada via variável de ambiente `PAINEL_SENHA` (padrão: `med123`)
- **Cada clínica deve ter sua própria senha configurada no Railway**

### Abas

**Cancelamentos:**
- Individual: digita CPF e cancela
- Lote: cola lista de CPFs, cancela todos
- Exporta resultado em CSV ou PDF

**Inclusões:**
- "Incluir por CPF (Tenex)": digita apenas o CPF, o sistema busca os dados no Tenex automaticamente — ideal para reprocessar clientes que falharam
- "Incluir um Paciente": formulário manual completo (bypass do Tenex)
- "Incluir Vários (Lote CSV)": cola tabela no formato `CPF;Nome;DataNasc;Sexo;NomeMae;CPFTitular`

**Histórico:**
- Exibe todas as operações registradas no banco
- Filtros por Tipo, Status e busca por nome/CPF
- Paginação (50 registros por página)
- A busca por nome/CPF funciona **localmente** nos dados da página atual (sem nova requisição)

---

## 11. Guia Multi-Clínica

O sistema foi projetado para isso. No Railway:

1. Crie um novo **Service** linkado ao mesmo repositório GitHub
2. Crie um novo **banco PostgreSQL** para essa clínica
3. Configure as variáveis de ambiente específicas dessa clínica
4. Qualquer manutenção no código (push no GitHub) é **automaticamente aplicada em todas as instâncias**

### Variáveis que mudam por clínica

- `TENEX_BASE_URL` — URL da clínica no Tenex
- `TENEX_BASIC_AUTH` — Token de autenticação do Tenex
- `MEDICAR_USERNAME` / `MEDICAR_PASSWORD` — Conta na Medicar
- `MEDICAR_CNPJMEDICAR` / `MEDICAR_GRUPOEMPRESA` / `MEDICAR_CONTRATO` — Identificação do contrato
- `MEDICAR_CONTRACT_FIELDS_JSON` — Campos BBA_* específicos da clínica
- `PLAN_MAPPING_JSON` — Mapeamento dos planos do Tenex para a Medicar
- `TENANT_ID` — Tenant da Medicar
- `DATABASE_URL` — Banco de dados exclusivo de cada clínica
- `PAINEL_SENHA` — Senha do painel web

---

## 12. Pontos de Atenção e Gotchas

### Token Medicar tem cache em memória
O `_token_cache` é um dicionário em memória. Se o servidor reiniciar (novo deploy), o cache é perdido e o próximo request busca um novo token. Isso é **intencional e correto**.

### Retry de 10 tentativas (60s cada) no webhook de novo cliente
O Tenex por vezes cadastra o cliente antes de associar o plano. Por isso o sistema tenta buscar a carteira virtual até 10 vezes com 60 segundos de intervalo. Uma inclusão pode demorar **até 10 minutos** para completar. Durante esse tempo, o registro aparece como "Aguardando..." no painel.

### `only_ascii_upper()` é obrigatória para nomes
A Medicar rejeita nomes com acentos, ç, etc. **Sempre use esta função** antes de enviar qualquer nome para a Medicar.

### CRÍTICO: cursor_factory no psycopg2
Ao chamar `conn.cursor()` no psycopg2, se quiser receber resultados como dicionários, você DEVE passar `cursor_factory=psycopg2.extras.RealDictCursor`. Sem isso, o retorno é uma tupla e qualquer acesso por chave (`row["campo"]`) falha com `TypeError`. Este foi o bug principal que causou falhas no histórico no início do projeto.

### Background Tasks vs threads
O FastAPI usa um event loop assíncrono. As Background Tasks rodam no mesmo processo de forma assíncrona. O `ThreadedConnectionPool` do psycopg2 existe porque operações de banco são síncronas e precisam de gerenciamento thread-safe.

### Webhook de dependentes é síncrono
O `/webhook/dependentes` processa tudo antes de retornar (diferente do `/webhook/novo-cliente`). Não tem retry de plano — assume que o plano já existe. Se der erro, o Tenex vai tentar reenviar automaticamente.

### `clientes_excluidos` previne reinclusão acidental
Quando um cliente é cancelado, ele é salvo nessa tabela. O webhook de dependentes verifica essa tabela antes de processar. Isso evita que uma atualização tardia de dependentes reinsira um cliente cancelado na Medicar.

---

## 13. Dependências Python

| Pacote | Por que é usado |
|---|---|
| `fastapi` | Framework web assíncrono para os endpoints |
| `uvicorn` | Servidor ASGI que roda o FastAPI |
| `httpx` | Cliente HTTP assíncrono para chamadas ao Tenex e Medicar |
| `psycopg2-binary` | Driver PostgreSQL para Python |
| `pydantic` | Validação dos modelos de request (BaseModel) |
| `python-multipart` | Suporte a form data (necessário para FastAPI) |
| `aiofiles` | Leitura assíncrona de arquivos (usado pelo StaticFiles) |

---

## 14. Logs

Os logs são escritos no stdout do container (visíveis no Railway em "Deployments > Logs").

**Padrão:** `YYYY-MM-DD HH:MM:SS,mmm [LEVEL] mensagem`

**Marcadores importantes:**
- `[WEBHOOK NOVO CLIENTE]` — Novo cliente recebido
- `[NOVO CLIENTE]` — Processamento em background de novo cliente
- `📩 WEBHOOK DEPENDENTES RECEBIDO` — Webhook de atualização recebido
- `[INCLUIR MANUAL]` / `[INCLUIR POR CPF]` — Inclusões manuais via painel
- `[CANCELAR]` — Operação de cancelamento
- `[HISTORICO]` — Erro ao listar histórico
- `[DB STARTUP]` — Problema ao iniciar o banco
- `✅` sucesso / `❌` erro

---

## 15. Como identificar e corrigir falhas

### Cliente não foi incluído na Medicar
1. Acesse o painel > aba Histórico > filtre por Status "Erro"
2. Veja a mensagem de erro
3. Se foi um erro temporário, use a aba Inclusões > "Incluir por CPF (Tenex)" com o CPF do cliente
4. O sistema vai rebuscar os dados no Tenex e tentar novamente

### O histórico está vazio mesmo após operações
A `DATABASE_URL` está errada ou o banco PostgreSQL está inacessível. Acesse `/debug/db` para ver o status da conexão.

### A API da Medicar está retornando erro
Veja os logs no Railway. O erro completo é logado em `[ERRO TITULAR] 422 → {corpo da resposta}`. O campo `errorMessage` no JSON de resposta explica o problema (CPF já cadastrado, campo faltando, etc.).

### Plano não encontrado após 10 tentativas
O cliente está no Tenex mas o plano não foi associado ainda. Verifique no Tenex se o contrato foi ativado. Se sim, use inclusão manual informando o plano diretamente.
