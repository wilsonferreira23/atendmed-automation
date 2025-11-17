from fastapi import FastAPI, Request, Query
import os, json, logging, asyncio, re
import httpx
from datetime import datetime, timedelta, date

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
# MEDICAR – TOKEN
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

# ============================================================
# MEDICAR – INCLUIR TITULAR (fluxo TOTVS)
# ============================================================
async def medicar_incluir_titular(
    token: str,
    tenantid: str,
    titular: dict,
    plano: dict,
    contract_fields: dict = None
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
# WEBHOOK PRINCIPAL – FLUXO COMPLETO
# ============================================================
@app.post("/webhook/clientes")
async def webhook_clientes(request: Request):
    body = await request.json()
    items = body if isinstance(body, list) else [body]

    log.info(f"Webhook recebido: {items}")

    # Token Medicar
    try:
        token = await medicar_get_token()
    except Exception as e:
        return {"status": "erro", "mensagem": f"Erro obtendo token: {e}"}

    tenantid = TENANT_ID

    # Caso tenant venha vazio → pega do contrato
    if not tenantid:
        try:
            contr = await medicar_get_contract(token)
            tenantid = contr.get("tenantid")
        except Exception:
            tenantid = None

    contract_fields = json.loads(MEDICAR_CONTRACT_FIELDS_JSON) if MEDICAR_CONTRACT_FIELDS_JSON else None

    results = []

    for item in items:
        data = item.get("data") or {}

        cpf = data.get("cpf")
        id_cliente = data.get("id")

        try:
            # === 1️⃣ Buscar plano na TENEX (com retry de 5 tentativas)
            carteira = None
            for tentativa in range(5):
                carteira = await tenex_get_carteira(cpf)
                if isinstance(carteira, list) and carteira and carteira[0].get("planos_contratados"):
                    break
                await asyncio.sleep(60)

            if not carteira or not carteira[0].get("planos_contratados"):
                results.append({"cpf": cpf, "status": "ignorado", "motivo": "sem plano"})
                continue

            pessoa = next((p for p in carteira if only_digits(p.get("cpf")) == only_digits(cpf)), carteira[0])
            id_plano = pessoa["planos_contratados"][0]["id_plano"]

            plano = PLAN_MAPPING_JSON.get(str(id_plano))
            if not plano:
                results.append({"cpf": cpf, "status": "ignorado", "motivo": f"plano {id_plano} não mapeado"})
                continue

            # === 2️⃣ Buscar titular e dependentes
            cliente_expand = await tenex_get_cliente_com_contatos(id_cliente)
            contatos = (cliente_expand or {}).get("contatos", [])

            tit = next((c for c in contatos if str(c.get("principal")) == "1"), None)
            titular_dict = {
                "nome": only_ascii_upper((tit or data).get("nome")),
                "cpf": only_digits((tit or data).get("cpf")),
                "data_nascimento": ( (tit or data).get("data_nascimento") or "" ).replace("-", ""),
                "sexo": str((tit or data).get("genero") or "2"),
                "nome_mae": "NOME MAE NAO INFORMADO",
            }

            dependentes_dicts = []
            for dep in contatos:
                if str(dep.get("principal")) != "0":
                    continue
                dependentes_dicts.append({
                    "nome": only_ascii_upper(dep.get("nome")),
                    "cpf": only_digits(dep.get("cpf")),
                    "data_nascimento": (dep.get("data_nascimento") or "").replace("-", ""),
                    "sexo": str(dep.get("genero") or "2"),
                    "nome_mae": "NOME MAE NAO INFORMADO",
                })

            if not titular_dict["nome"] or not titular_dict["cpf"]:
                results.append({"cpf": cpf, "status": "erro", "erro": "Titular inválido"})
                continue

            # === 3️⃣ INCLUIR TITULAR NA MEDICAR
            resp_titular = await medicar_incluir_titular(
                token=token,
                tenantid=tenantid,
                titular=titular_dict,
                plano=plano,
                contract_fields=contract_fields
            )

            log.info(f"Titular incluído → CPF {titular_dict['cpf']}")

            # === 4️⃣ Buscar a matrícula recém-criada
            url_mat = f"{MEDICAR_BASE_URL}/client/v1/contract"
            headers = {"Authorization": f"Bearer {token}"}
            params = {
                "cnpjmedicar": MEDICAR_CNPJMEDICAR,
                "grupoempresa": MEDICAR_GRUPOEMPRESA,
                "contrato": MEDICAR_CONTRATO,
                "cgcbeneficiario": titular_dict["cpf"]
            }

            resp_mat = await httpx_retry("GET", url_mat, headers=headers, params=params)
            contr_data = resp_mat.json()

            matricula = contr_data.get("BBA_MATRIC")
            tenant_dep = contr_data.get("tenantid")

            if not matricula:
                results.append({"cpf": cpf, "status": "erro", "erro": "Sem matrícula retornada"})
                continue

            # === 5️⃣ Incluir dependentes (se houver)
            if dependentes_dicts:
                resp_dep = await medicar_incluir_dependentes(
                    token=token,
                    tenantid=tenant_dep,
                    matricula=matricula,
                    dependentes=dependentes_dicts
                )
            else:
                resp_dep = {"msg": "Sem dependentes"}

            results.append({
                "cpf": titular_dict["cpf"],
                "status": "cadastrado",
                "titular": resp_titular,
                "dependentes": resp_dep
            })

        except Exception as e:
            results.append({
                "cpf": cpf,
                "status": "erro",
                "erro": str(e)
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
    except:
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
# CANCELAR
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
    tenantid = contract.get("tenantid")

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
