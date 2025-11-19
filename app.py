from fastapi import FastAPI, Request, Query
import os, json, logging, asyncio, re, sqlite3
import httpx
from datetime import datetime, timedelta, date
from contextlib import contextmanager

app = FastAPI(title="Atende Med – Integração TENEX → MEDICAR (async)")

# ============================================================
# LOGS
# ============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("atendmed-api")

# ============================================================
# UTIL
# ============================================================
def only_digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def only_ascii_upper(s: str) -> str:
    return (s or "").encode("ascii", errors="ignore").decode().upper().strip()

# ============================================================
# CONFIG
# ============================================================
TENEX_BASE_URL = os.getenv("TENEX_BASE_URL", "https://maisaudebh.tenex.com.br").rstrip("/")
TENEX_BASIC_AUTH = os.getenv("TENEX_BASIC_AUTH")

MEDICAR_BASE_URL = os.getenv("MEDICAR_BASE_URL", "").rstrip("/")
MEDICAR_USERNAME = os.getenv("MEDICAR_USERNAME")
MEDICAR_PASSWORD = os.getenv("MEDICAR_PASSWORD")

MEDICAR_CNPJMEDICAR = only_digits(os.getenv("MEDICAR_CNPJMEDICAR", ""))
MEDICAR_GRUPOEMPRESA = os.getenv("MEDICAR_GRUPOEMPRESA")
MEDICAR_CONTRATO = os.getenv("MEDICAR_CONTRATO")

TENANT_ID = os.getenv("TENANT_ID")

MEDICAR_CONTRACT_FIELDS_JSON = os.getenv("MEDICAR_CONTRACT_FIELDS_JSON", "")

PLAN_MAPPING_JSON = json.loads(os.getenv(
    "PLAN_MAPPING_JSON",
    '{"34":{"codpro":"0066","versao":"001"},"35":{"codpro":"0066","versao":"001"}}'
))

HTTP_TIMEOUT = 25.0
_token_cache = {"token": None, "expiry": datetime.min}

# ============================================================
# DB – clientes_excluidos (SQLite simples)
# ============================================================
DB_PATH = os.getenv("DELETED_DB_PATH", "clientes_excluidos.db")

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS clientes_excluidos (
                id_cliente INTEGER PRIMARY KEY,
                cpf TEXT NOT NULL,
                data_exclusao TEXT NOT NULL
            )
        """)
        conn.commit()

def db_salvar_excluido(id_cliente: int, cpf: str):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO clientes_excluidos (id_cliente, cpf, data_exclusao)
            VALUES (?, ?, ?)
            """,
            (id_cliente, cpf, datetime.utcnow().isoformat())
        )
        conn.commit()

def db_buscar_excluido(id_cliente: int):
    with get_conn() as conn:
        cur = conn.execute(
            "SELECT id_cliente, cpf, data_exclusao FROM clientes_excluidos WHERE id_cliente = ?",
            (id_cliente,)
        )
        row = cur.fetchone()
        if row:
            return dict(row)
        return None

def db_remover_excluido(id_cliente: int):
    with get_conn() as conn:
        conn.execute("DELETE FROM clientes_excluidos WHERE id_cliente = ?", (id_cliente,))
        conn.commit()

# inicializa a tabela na importação
init_db()

# ============================================================
# HTTP RETRY
# ============================================================
async def httpx_retry(method: str, url: str, **kwargs) -> httpx.Response:
    tries, delay = 3, 1.0
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        for i in range(tries):
            try:
                resp = await client.request(method, url, **kwargs)
                resp.raise_for_status()
                return resp
            except httpx.HTTPError as e:
                if i == tries - 1:
                    raise
                log.warning(f"Tentativa {i+1}/{tries} falhou para {url}: {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 6)

# ============================================================
# TENEX
# ============================================================
async def tenex_get_carteira(cpf: str):
    url = f"{TENEX_BASE_URL}/api/v2/carteira-virtual/{only_digits(cpf)}"
    headers = {"Authorization": f"Basic {TENEX_BASIC_AUTH}"}
    resp = await httpx_retry("GET", url, headers=headers)
    return resp.json()

async def tenex_get_cliente_com_contatos(cliente_id: int):
    url = f"{TENEX_BASE_URL}/api/v2/clientes/?id={cliente_id}&_expand=contatos"
    headers = {"Authorization": f"Basic {TENEX_BASIC_AUTH}"}
    resp = await httpx_retry("GET", url, headers=headers)
    data = resp.json()
    if isinstance(data, dict) and "data" in data:
        items = data["data"]
    else:
        items = data
    return items[0] if items else None

# ============================================================
# MEDICAR – TOKEN / CONTRATO
# ============================================================
async def medicar_get_token():
    if _token_cache["token"] and datetime.now() < _token_cache["expiry"]:
        return _token_cache["token"]

    url = f"{MEDICAR_BASE_URL}/api/oauth2/v1/token"
    params = {"grant_type": "password", "username": MEDICAR_USERNAME, "password": MEDICAR_PASSWORD}

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.post(url, params=params)
        resp.raise_for_status()

    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"Token inválido: {data}")

    ttl = int(data.get("expires_in", 3600)) - 60
    _token_cache["token"] = token
    _token_cache["expiry"] = datetime.now() + timedelta(seconds=max(ttl, 60))

    log.info("✅ Token Medicar obtido com sucesso.")
    return token

async def medicar_get_contract(token: str):
    """
    Busca contrato padrão da Medicar para obter tenantid, etc.
    Usado como fallback quando TENANT_ID não vem por env.
    """
    url = f"{MEDICAR_BASE_URL}/client/v1/contract"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "cnpjmedicar": MEDICAR_CNPJMEDICAR,
        "grupoempresa": MEDICAR_GRUPOEMPRESA,
        "contrato": MEDICAR_CONTRATO,
    }
    resp = await httpx_retry("GET", url, headers=headers, params=params)
    return resp.json()

# ============================================================
# MEDICAR – INCLUIR TITULAR (fluxo TOTVS)
# ============================================================
async def medicar_incluir_titular(
    token: str,
    tenantid: str,
    titular: dict,
    plano: dict,
    contract_fields: dict | None = None
):
    """
    Inclui APENAS o titular na Medicar.
    Depois buscamos a matrícula e incluímos dependentes separadamente.
    """

    url = f"{MEDICAR_BASE_URL}/fwmodel/PLIncBenModel/"
    params = {"tenantId": tenantid}

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "tenantid": tenantid
    }

    # Formatação
    def fmt_dn(v): return v.replace("-", "") if v else ""
    def sexo_valor(v): return "1" if str(v) == "1" else "2"
    def mae_ok(v): return only_ascii_upper(v or "NOME MAE NAO INFORMADO")

    # Campos default da Medicar
    master_defaults = {
        "BBA_CODINT": "1001",
        "BBA_CODEMP": "0004",
        "BBA_CONEMP": "000000000002",
        "BBA_VERCON": "001",
        "BBA_SUBCON": "002326875",
        "BBA_VERSUB": "001",
    }

    env_contract = {k: contract_fields.get(k) for k in master_defaults if contract_fields and contract_fields.get(k)}
    base_contract = env_contract if env_contract else master_defaults

    nome = only_ascii_upper(titular["nome"])
    cpf = only_digits(titular["cpf"])
    dn = fmt_dn(titular["data_nascimento"])
    sexo = sexo_valor(titular["sexo"])
    mae = mae_ok(titular["nome_mae"])

    # Campos MASTERBBA
    master_bba_fields = [
        {"id": "BBA_CODINT", "order": 1, "value": base_contract["BBA_CODINT"]},
        {"id": "BBA_CODEMP", "order": 2, "value": base_contract["BBA_CODEMP"]},
        {"id": "BBA_CONEMP", "order": 3, "value": base_contract["BBA_CONEMP"]},
        {"id": "BBA_VERCON", "order": 4, "value": base_contract["BBA_VERCON"]},
        {"id": "BBA_SUBCON", "order": 5, "value": base_contract["BBA_SUBCON"]},
        {"id": "BBA_VERSUB", "order": 6, "value": base_contract["BBA_VERSUB"]},
        {"id": "BBA_EMPBEN", "order": 7, "value": nome},
        {"id": "BBA_CODPRO", "order": 8, "value": plano["codpro"]},
        {"id": "BBA_VERSAO", "order": 9, "value": plano["versao"]},
        {"id": "BBA_CPFTIT", "order": 10, "value": cpf},
    ]

    # DETAIL – titular
    items = [{
        "id": 1,
        "deleted": 0,
        "fields": [
            {"id": "B2N_NOMUSR", "value": nome},
            {"id": "B2N_DATNAS", "value": dn},
            {"id": "B2N_GRAUPA", "value": "00"},
            {"id": "B2N_ESTCIV", "value": "S"},
            {"id": "B2N_SEXO", "value": sexo},
            {"id": "B2N_CPFUSR", "value": cpf},
            {"id": "B2N_MAE", "value": mae},
            {"id": "B2N_CODPRO", "value": plano["codpro"]},
        ]
    }]

    payload = {
        "id": "PLIncBenModel",
        "operation": 3,
        "models": [{
            "id": "MASTERBBA",
            "modeltype": "FIELDS",
            "fields": master_bba_fields,
            "models": [
                {"id": "DETAILB2N", "modeltype": "GRID", "items": items},
                {"id": "DETAILANEXO", "modeltype": "GRID", "items": [{"id": 1, "deleted": 0, "fields": []}]},
            ],
        }],
    }

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.post(url, params=params, headers=headers, json=payload)

    try:
        resp.raise_for_status()
    except Exception:
        log.error(f"[ERRO TITULAR] {resp.status_code} → {resp.text}")
        raise

    return resp.json()

# ============================================================
# MEDICAR – INCLUIR DEPENDENTES
# ============================================================
async def medicar_incluir_dependentes(
    token: str,
    tenantid: str,
    matricula: str,
    dependentes: list
):
    """
    Inclui dependentes em um titular já existente na Medicar.
    """
    url = f"{MEDICAR_BASE_URL}/fwmodel/PLIncBenModel/"
    params = {"tenantId": tenantid}

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "tenantid": tenantid
    }

    items = []

    for i, dep in enumerate(dependentes, start=1):

        nome = only_ascii_upper(dep.get("nome") or "")
        data_nas = dep.get("data_nascimento") or ""
        sexo = str(dep.get("sexo") or "2")
        cpf = only_digits(dep.get("cpf") or "")
        mae = only_ascii_upper(dep.get("nome_mae") or "NOME MAE NAO INFORMADO")

        if not nome or not cpf or not data_nas:
            log.warning(f"[DEPENDENTE IGNORADO] Falta nome/cpf/data")
            continue

        items.append({
            "id": i,
            "deleted": 0,
            "fields": [
                {"id": "B2N_NOMUSR", "value": nome},
                {"id": "B2N_DATNAS", "value": data_nas},
                {"id": "B2N_GRAUPA", "value": "11"},
                {"id": "B2N_ESTCIV", "value": "S"},
                {"id": "B2N_SEXO", "value": sexo},
                {"id": "B2N_CPFUSR", "value": cpf},
                {"id": "B2N_MAE", "value": mae}
            ]
        })

    payload = {
        "id": "PLIncBenModel",
        "operation": 3,
        "models": [{
            "id": "MASTERBBA",
            "modeltype": "FIELDS",
            "fields": [
                {"id": "BBA_MATRIC", "order": 1, "value": matricula}
            ],
            "models": [
                {"id": "DETAILB2N", "modeltype": "GRID", "items": items},
                {"id": "DETAILANEXO", "modeltype": "GRID", "items": [{"id": 1, "deleted": 0, "fields": []}]}
            ]
        }]
    }

    log.info(f"[MEDICAR] Incluindo {len(items)} dependente(s) → matrícula {matricula}")

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.post(url, params=params, headers=headers, json=payload)

    try:
        resp.raise_for_status()
    except Exception:
        log.error(f"[ERRO DEPENDENTES] {resp.status_code} → {resp.text}")
        raise

    return resp.json()

# ============================================================
# MEDICAR – CANCELAR MATRÍCULA
# ============================================================
async def medicar_encerrar_matricula(
    token: str,
    subscriber_id: str,
    reason: str,
    block_date: str,
    login_user: str
):

    url = f"{MEDICAR_BASE_URL}/totvsHealthPlans/familyContract/v1/beneficiaries/blockProtocol"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "tenantid": TENANT_ID
    }

    payload = {
        "subscriberId": subscriber_id,
        "reason": reason,
        "blockDate": block_date,
        "loginUser": login_user
    }

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.post(url, headers=headers, json=payload)

    try:
        resp.raise_for_status()
    except Exception:
        log.error(f"[ERRO CANCELAR] {resp.text}")
        raise

    return resp.json()

# ============================================================
# HELPER – PROCESSAR UM ITEM COMO "NOVO CLIENTE"
# ============================================================
async def process_novo_cliente_item(
    item: dict,
    token: str,
    tenantid: str,
    contract_fields: dict | None
) -> dict:
    header = item.get("header") or {}
    data = item.get("data") or {}

    cpf = data.get("cpf")
    id_cliente = data.get("id")

    if not cpf or not id_cliente:
        return {
            "status": "erro",
            "motivo": "Webhook sem cpf ou id_cliente em data",
            "data": data
        }

    try:
        # 1️⃣ Buscar plano na TENEX (com retry de 5 tentativas)
        carteira = None
        for tentativa in range(5):
            carteira = await tenex_get_carteira(cpf)
            if isinstance(carteira, list) and carteira and carteira[0].get("planos_contratados"):
                log.info(f"[NOVO CLIENTE] Plano encontrado na tentativa {tentativa+1} para CPF {cpf}")
                break
            log.warning(f"[NOVO CLIENTE] Tentativa {tentativa+1}/5: plano não disponível para CPF {cpf}. Aguardando 60 s...")
            await asyncio.sleep(60)

        if not carteira or not carteira[0].get("planos_contratados"):
            return {
                "cpf": cpf,
                "status": "ignorado",
                "motivo": "Nenhum plano encontrado após 5 tentativas"
            }

        pessoa = next((p for p in carteira if only_digits(p.get("cpf", "")) == only_digits(cpf)), carteira[0])
        id_plano = pessoa["planos_contratados"][0]["id_plano"]

        plano = PLAN_MAPPING_JSON.get(str(id_plano))
        if not plano:
            return {
                "cpf": cpf,
                "status": "ignorado",
                "motivo": f"plano {id_plano} não mapeado"
            }

        # 2️⃣ Buscar titular e dependentes no TENEX
        cliente_expand = await tenex_get_cliente_com_contatos(id_cliente)
        contatos = (cliente_expand or {}).get("contatos", []) if cliente_expand else []

        tit = next((c for c in contatos if str(c.get("principal")) == "1"), None)
        titular_dict = {
            "nome": only_ascii_upper((tit or data).get("nome") or ""),
            "cpf": only_digits((tit or data).get("cpf") or ""),
            "data_nascimento": ((tit or data).get("data_nascimento") or "").replace("-", ""),
            "sexo": str((tit or data).get("genero") or "2"),
            "nome_mae": "NOME MAE NAO INFORMADO",
        }

        dependentes_dicts = []
        for dep in contatos:
            if str(dep.get("principal")) != "0":
                continue
            if not dep.get("cpf"):
                continue
            dependentes_dicts.append({
                "nome": only_ascii_upper(dep.get("nome") or ""),
                "cpf": only_digits(dep.get("cpf") or ""),
                "data_nascimento": (dep.get("data_nascimento") or "").replace("-", ""),
                "sexo": str(dep.get("genero") or "2"),
                "nome_mae": "NOME MAE NAO INFORMADO",
            })

        if not titular_dict["nome"] or not titular_dict["cpf"]:
            return {"cpf": cpf, "status": "erro", "erro": "Titular inválido (sem nome/CPF)"}

        # 3️⃣ Incluir TITULAR na Medicar
        resp_titular = await medicar_incluir_titular(
            token=token,
            tenantid=tenantid,
            titular=titular_dict,
            plano=plano,
            contract_fields=contract_fields,
        )

        log.info(f"[NOVO CLIENTE] Titular incluído → CPF {titular_dict['cpf']}")

        # 4️⃣ Buscar a matrícula recém-criada
        url_mat = f"{MEDICAR_BASE_URL}/client/v1/contract"
        headers_medicar = {"Authorization": f"Bearer {token}"}
        params_mat = {
            "cnpjmedicar": MEDICAR_CNPJMEDICAR,
            "grupoempresa": MEDICAR_GRUPOEMPRESA,
            "contrato": MEDICAR_CONTRATO,
            "cgcbeneficiario": only_digits(titular_dict["cpf"]),
        }

        resp_mat = await httpx_retry("GET", url_mat, headers=headers_medicar, params=params_mat)
        contr_data = resp_mat.json()

        matricula = contr_data.get("BBA_MATRIC")
        tenant_dep = contr_data.get("tenantid") or tenantid

        if not matricula:
            return {
                "cpf": cpf,
                "status": "erro",
                "erro": "Titular criado mas matrícula não retornou (BBA_MATRIC vazio)"
            }

        # 5️⃣ Incluir DEPENDENTES (se houver)
        if dependentes_dicts:
            resp_dep = await medicar_incluir_dependentes(
                token=token,
                tenantid=tenant_dep,
                matricula=matricula,
                dependentes=dependentes_dicts,
            )
        else:
            resp_dep = {"mensagem": "Nenhum dependente encontrado"}

        return {
            "cpf": titular_dict["cpf"],
            "status": "cadastrado",
            "titular": resp_titular,
            "dependentes": resp_dep,
        }

    except Exception as e:
        log.exception(f"[NOVO CLIENTE] Erro ao processar CPF {cpf}")
        return {
            "cpf": cpf,
            "status": "erro",
            "erro": str(e),
        }

# ============================================================
# 1) WEBHOOK – NOVO CLIENTE (insert)
# ============================================================
@app.post("/webhook/novo-cliente")
async def webhook_novo_cliente(request: Request):
    body = await request.json()
    items = body if isinstance(body, list) else [body]

    log.info(f"[WEBHOOK NOVO CLIENTE] Recebido: {items}")

    # Token Medicar
    try:
        token = await medicar_get_token()
    except Exception as e:
        return {"status": "erro", "mensagem": f"Erro obtendo token: {e}"}

    tenantid = TENANT_ID

    # Caso tenant venha vazio → pega do contrato padrão
    if not tenantid:
        try:
            contr = await medicar_get_contract(token)
            tenantid = contr.get("tenantid")
        except Exception as e:
            log.warning(f"Não foi possível obter tenant padrão: {e}")
            tenantid = None

    contract_fields = json.loads(MEDICAR_CONTRACT_FIELDS_JSON) if MEDICAR_CONTRACT_FIELDS_JSON else None
    results = []

    for item in items:
        header = item.get("header") or {}
        op = (header.get("operation") or "").lower()
        if op and op != "insert":
            results.append({
                "status": "ignorado",
                "motivo": f"operation diferente de insert ({op})",
                "raw_header": header
            })
            continue

        result_item = await process_novo_cliente_item(
            item=item,
            token=token,
            tenantid=tenantid,
            contract_fields=contract_fields
        )
        results.append(result_item)

    return {"status": "ok", "resultados": results}

# ============================================================
# 2) WEBHOOK – ATUALIZAÇÃO / DEPENDENTES (update + delete)
# ============================================================
@app.post("/webhook/dependentes")
async def webhook_dependentes(request: Request):
    body = await request.json()
    items = body if isinstance(body, list) else [body]

    log.info(f"[WEBHOOK DEPENDENTES] Recebido: {items}")

    try:
        token = await medicar_get_token()
    except Exception as e:
        return {"status": "erro", "mensagem": f"Erro obtendo token: {e}"}

    # tenant para reentrada (novo-cliente interno)
    tenantid = TENANT_ID
    if not tenantid:
        try:
            contr = await medicar_get_contract(token)
            tenantid = contr.get("tenantid")
        except Exception as e:
            log.warning(f"Não foi possível obter tenant padrão no webhook/dependentes: {e}")
            tenantid = None

    contract_fields = json.loads(MEDICAR_CONTRACT_FIELDS_JSON) if MEDICAR_CONTRACT_FIELDS_JSON else None

    results = []

    for item in items:
        header = item.get("header") or {}
        data = item.get("data") or {}

        op = (header.get("operation") or "").lower()
        if op and op != "update":
            results.append({
                "status": "ignorado",
                "motivo": f"operation diferente de update ({op})",
                "raw_header": header
            })
            continue

        cpf = data.get("cpf")
        id_cliente = data.get("id")

        if not cpf or not id_cliente:
            results.append({
                "status": "erro",
                "motivo": "Webhook sem cpf ou id_cliente em data",
                "data": data
            })
            continue

        cpf_digits = only_digits(cpf)

        try:
            # 1️⃣ Buscar cliente + contatos + status no TENEX
            cliente_expand = await tenex_get_cliente_com_contatos(id_cliente)
            if not cliente_expand:
                results.append({
                    "cpf": cpf_digits,
                    "status": "erro",
                    "erro": "Cliente não encontrado no TENEX pelo id"
                })
                continue

            status_tenex = cliente_expand.get("status")
            contatos = (cliente_expand or {}).get("contatos", []) if cliente_expand else []

            log.info(f"[WEBHOOK DEPENDENTES] Cliente {id_cliente} status={status_tenex}")

            # 🔴 2️⃣ Se STATUS = 2 → trata como EXCLUSÃO (delete)
            if status_tenex == 2:
                log.info(f"[WEBHOOK DEPENDENTES] Cliente {id_cliente} com status=2 → fluxo de exclusão")

                # Verificar plano antes de cancelar
                carteira = await tenex_get_carteira(cpf_digits)
                first = carteira[0] if isinstance(carteira, list) and carteira else None

                if not first or not first.get("planos_contratados"):
                    results.append({
                        "cpf": cpf_digits,
                        "status": "ignorado",
                        "motivo": "Cliente sem plano ativo na exclusão"
                    })
                    continue

                pessoa = next((p for p in carteira if only_digits(p.get("cpf", "")) == cpf_digits), first)
                id_plano = pessoa["planos_contratados"][0]["id_plano"]
                plano = PLAN_MAPPING_JSON.get(str(id_plano))
                if not plano:
                    results.append({
                        "cpf": cpf_digits,
                        "status": "ignorado",
                        "motivo": f"plano {id_plano} não mapeado na exclusão"
                    })
                    continue

                # Buscar matrícula na Medicar
                url_mat = f"{MEDICAR_BASE_URL}/client/v1/contract"
                headers_medicar = {"Authorization": f"Bearer {token}"}
                params_mat = {
                    "cnpjmedicar": MEDICAR_CNPJMEDICAR,
                    "grupoempresa": MEDICAR_GRUPOEMPRESA,
                    "contrato": MEDICAR_CONTRATO,
                    "cgcbeneficiario": cpf_digits,
                }

                resp_mat = await httpx_retry("GET", url_mat, headers=headers_medicar, params=params_mat)
                contr_data = resp_mat.json()

                subscriber_id = contr_data.get("BBA_MATRIC")

                if not subscriber_id:
                    results.append({
                        "cpf": cpf_digits,
                        "status": "ignorado",
                        "motivo": "Não há matrícula (BBA_MATRIC) para cancelar"
                    })
                    continue

                block_date = date.today().strftime("%Y-%m-%d")

                resp_cancel = await medicar_encerrar_matricula(
                    token=token,
                    subscriber_id=subscriber_id,
                    reason="000001",
                    block_date=block_date,
                    login_user="WEBHOOK DEPENDENTES (EXCLUSAO)"
                )

                # Salva no banco de excluídos
                db_salvar_excluido(id_cliente=id_cliente, cpf=cpf_digits)

                log.info(f"[WEBHOOK DEPENDENTES] Matrícula cancelada e cliente {id_cliente} marcado como excluído")

                results.append({
                    "cpf": cpf_digits,
                    "status": "cancelado",
                    "subscriberId": subscriber_id,
                    "resultado": resp_cancel
                })
                continue

            # 🟢 3️⃣ STATUS = 1 → cliente ATIVO
            #    Verificar se ele está marcado como excluído (reentrada)
            excluido = db_buscar_excluido(id_cliente=id_cliente)
            if excluido:
                log.info(f"[WEBHOOK DEPENDENTES] Cliente {id_cliente} estava excluído e foi reativado → fluxo de novo cliente")

                # Reaproveita a automação do /webhook/novo-cliente
                result_item = await process_novo_cliente_item(
                    item=item,
                    token=token,
                    tenantid=tenantid,
                    contract_fields=contract_fields
                )

                # Remove do banco de excluídos
                db_remover_excluido(id_cliente=id_cliente)

                result_item["reentrada"] = True
                results.append(result_item)
                continue

            # 4️⃣ Fluxo NORMAL de atualização de dependentes (cliente ativo)
            #    Verificar plano na TENEX
            carteira = await tenex_get_carteira(cpf_digits)
            first = carteira[0] if isinstance(carteira, list) and carteira else None

            if not first or not first.get("planos_contratados"):
                results.append({
                    "cpf": cpf_digits,
                    "status": "ignorado",
                    "motivo": "Cliente sem plano ativo"
                })
                continue

            pessoa = next((p for p in carteira if only_digits(p.get("cpf", "")) == cpf_digits), first)
            id_plano = pessoa["planos_contratados"][0]["id_plano"]
            plano = PLAN_MAPPING_JSON.get(str(id_plano))
            if not plano:
                results.append({
                    "cpf": cpf_digits,
                    "status": "ignorado",
                    "motivo": f"plano {id_plano} não mapeado"
                })
                continue

            # Montar dependentes a partir dos contatos
            dependentes_dicts = []
            for dep in contatos:
                if str(dep.get("principal")) != "0":
                    continue
                if not dep.get("cpf"):
                    continue
                dependentes_dicts.append({
                    "nome": only_ascii_upper(dep.get("nome") or ""),
                    "cpf": only_digits(dep.get("cpf") or ""),
                    "data_nascimento": (dep.get("data_nascimento") or "").replace("-", ""),
                    "sexo": str(dep.get("genero") or "2"),
                    "nome_mae": "NOME MAE NAO INFORMADO",
                })

            if not dependentes_dicts:
                results.append({
                    "cpf": cpf_digits,
                    "status": "ignorado",
                    "motivo": "Nenhum dependente encontrado nos contatos"
                })
                continue

            # Buscar matrícula na Medicar
            url_mat = f"{MEDICAR_BASE_URL}/client/v1/contract"
            headers_medicar = {"Authorization": f"Bearer {token}"}
            params_mat = {
                "cnpjmedicar": MEDICAR_CNPJMEDICAR,
                "grupoempresa": MEDICAR_GRUPOEMPRESA,
                "contrato": MEDICAR_CONTRATO,
                "cgcbeneficiario": cpf_digits,
            }

            resp_mat = await httpx_retry("GET", url_mat, headers=headers_medicar, params=params_mat)
            contr_data = resp_mat.json()

            matricula = contr_data.get("BBA_MATRIC")
            tenant_dep = contr_data.get("tenantid") or TENANT_ID

            if not matricula:
                results.append({
                    "cpf": cpf_digits,
                    "status": "erro",
                    "erro": "Não foi possível obter matrícula (BBA_MATRIC) para o titular"
                })
                continue

            # Incluir/atualizar dependentes
            resp_dep = await medicar_incluir_dependentes(
                token=token,
                tenantid=tenant_dep,
                matricula=matricula,
                dependentes=dependentes_dicts,
            )

            results.append({
                "cpf": cpf_digits,
                "status": "dependentes_atualizados",
                "dependentes": resp_dep,
            })

        except Exception as e:
            log.exception(f"[WEBHOOK DEPENDENTES] Erro ao processar CPF {cpf_digits}")
            results.append({
                "cpf": cpf_digits,
                "status": "erro",
                "erro": str(e),
            })

    return {"status": "ok", "resultados": results}

# ============================================================
# ENDPOINT DE TESTE – incluir dependentes manualmente
# ============================================================
@app.post("/adicionar-dependentes")
async def adicionar_dependentes(
    cpf_titular: str = Query(...),
    dependentes: str = Query(...)
):
    cpf_digits = only_digits(cpf_titular)

    try:
        dependentes_list = json.loads(dependentes)
    except Exception:
        return {"status": "erro", "mensagem": "JSON inválido"}

    token = await medicar_get_token()

    # Buscar matrícula
    url = f"{MEDICAR_BASE_URL}/client/v1/contract"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "cnpjmedicar": MEDICAR_CNPJMEDICAR,
        "grupoempresa": MEDICAR_GRUPOEMPRESA,
        "contrato": MEDICAR_CONTRATO,
        "cgcbeneficiario": cpf_digits
    }

    resp = await httpx_retry("GET", url, headers=headers, params=params)
    contract = resp.json()

    matricula = contract.get("BBA_MATRIC")
    tenantid = contract.get("tenantid")

    if not matricula:
        return {"status": "erro", "mensagem": "Sem BBA_MATRIC"}

    result = await medicar_incluir_dependentes(
        token=token,
        tenantid=tenantid,
        matricula=matricula,
        dependentes=dependentes_list
    )

    return {
        "status": "ok",
        "cpf": cpf_digits,
        "matricula": matricula,
        "resultado": result
    }

# ============================================================
# CANCELAR – TESTE MANUAL POR CPF
# ============================================================
@app.post("/cancelar-por-cpf")
async def cancelar_por_cpf(
    cpf: str = Query(...),
    reason: str = Query("000001"),
    loginUser: str = Query("USUARIO API")
):
    cpf_digits = only_digits(cpf)
    token = await medicar_get_token()

    # Buscar matrícula
    url = f"{MEDICAR_BASE_URL}/client/v1/contract"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "cnpjmedicar": MEDICAR_CNPJMEDICAR,
        "grupoempresa": MEDICAR_GRUPOEMPRESA,
        "contrato": MEDICAR_CONTRATO,
        "cgcbeneficiario": cpf_digits
    }

    resp = await httpx_retry("GET", url, headers=headers, params=params)
    contract = resp.json()

    subscriberId = contract.get("BBA_MATRIC")

    if not subscriberId:
        return {"status": "erro", "mensagem": "Sem subscriberId"}

    result = await medicar_encerrar_matricula(
        token=token,
        subscriber_id=subscriberId,
        reason=reason,
        block_date=date.today().strftime("%Y-%m-%d"),
        login_user=loginUser
    )

    return {
        "status": "ok",
        "cpf": cpf_digits,
        "subscriberId": subscriberId,
        "resultado": result
    }

# ============================================================
# HEALTHCHECK
# ============================================================
@app.get("/health")
async def health():
    return {"status": "online", "servico": "TENEX → MEDICAR (async)"}
