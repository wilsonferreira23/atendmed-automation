from fastapi import FastAPI, Request
from pydantic import BaseModel
import os, json, logging, asyncio, re
import httpx
from datetime import datetime, timedelta

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
# MEDICAR – TOKEN, CONTRATO, INCLUSÃO FAMÍLIA
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
    url = f"{MEDICAR_BASE_URL}/client/v1/contract"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"cnpjmedicar": MEDICAR_CNPJMEDICAR, "grupoempresa": MEDICAR_GRUPOEMPRESA, "contrato": MEDICAR_CONTRATO}
    resp = await httpx_retry("GET", url, headers=headers, params=params)
    return resp.json()

async def medicar_incluir_familia(token, tenantid, titular, dependentes, plano, contract_fields=None):
    url = f"{MEDICAR_BASE_URL}/fwmodel/PLIncBenModel/"
    params = {"tenantId": tenantid}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def fmt_dn(v): return v.replace("-", "") if v else ""
    def sexo_valor(v): return "1" if str(v) == "1" else "2"
    def mae_ok(v): return only_ascii_upper(v or "NOME MAE NAO INFORMADO")

    master_defaults = {
        "BBA_CODINT": "1001", "BBA_CODEMP": "0004", "BBA_CONEMP": "000000000002",
        "BBA_VERCON": "001", "BBA_SUBCON": "002326875", "BBA_VERSUB": "001",
    }
    env_contract = {k: contract_fields.get(k) for k in master_defaults if contract_fields and contract_fields.get(k)}
    base_contract = env_contract if env_contract else master_defaults

    nome_tit = only_ascii_upper(titular.get("nome"))
    cpf_tit = only_digits(titular.get("cpf"))
    dn_tit = fmt_dn(titular.get("data_nascimento"))
    sx_tit = sexo_valor(titular.get("sexo"))
    mae_tit = mae_ok(titular.get("nome_mae"))

    master_bba_fields = [
        {"id": "BBA_CODINT", "order": 1, "value": base_contract["BBA_CODINT"]},
        {"id": "BBA_CODEMP", "order": 2, "value": base_contract["BBA_CODEMP"]},
        {"id": "BBA_CONEMP", "order": 3, "value": base_contract["BBA_CONEMP"]},
        {"id": "BBA_VERCON", "order": 4, "value": base_contract["BBA_VERCON"]},
        {"id": "BBA_SUBCON", "order": 5, "value": base_contract["BBA_SUBCON"]},
        {"id": "BBA_VERSUB", "order": 6, "value": base_contract["BBA_VERSUB"]},
        {"id": "BBA_EMPBEN", "order": 7, "value": nome_tit},
        {"id": "BBA_CODPRO", "order": 8, "value": plano["codpro"]},
        {"id": "BBA_VERSAO", "order": 9, "value": plano["versao"]},
        {"id": "BBA_CPFTIT", "order": 10, "value": cpf_tit},
    ]

    items = [{
        "id": 1, "deleted": 0, "fields": [
            {"id": "B2N_NOMUSR", "value": nome_tit},
            {"id": "B2N_DATNAS", "value": dn_tit},
            {"id": "B2N_GRAUPA", "value": "00"},
            {"id": "B2N_ESTCIV", "value": "S"},
            {"id": "B2N_SEXO", "value": sx_tit},
            {"id": "B2N_CPFUSR", "value": cpf_tit},
            {"id": "B2N_MAE", "value": mae_tit},
            {"id": "B2N_CODPRO", "value": plano["codpro"]},
        ]
    }]

    for i, dep in enumerate(dependentes, start=2):
        nome = only_ascii_upper(dep.get("nome"))
        cpf = only_digits(dep.get("cpf"))
        dn = fmt_dn(dep.get("data_nascimento"))
        sx = sexo_valor(dep.get("sexo"))
        mae = mae_ok(dep.get("nome_mae"))
        if not nome or not cpf:
            continue
        items.append({
            "id": i, "deleted": 0, "fields": [
                {"id": "B2N_NOMUSR", "value": nome},
                {"id": "B2N_DATNAS", "value": dn},
                {"id": "B2N_GRAUPA", "value": "11"},
                {"id": "B2N_ESTCIV", "value": "S"},
                {"id": "B2N_SEXO", "value": sx},
                {"id": "B2N_CPFUSR", "value": cpf},
                {"id": "B2N_MAE", "value": mae},
            ]
        })

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
        resp.raise_for_status()
        return resp.json()

# ============================================================
# MEDICAR – ENCERRAR MATRÍCULA (porta 1356, parametrizado)
# ============================================================
async def medicar_encerrar_matricula(
    subscriber_id: str | None = None,
    reason: str = "000001",
    block_date: str | None = None,
    login_user: str | None = None
):
    """
    Executa o encerramento de matrícula na Medicar (porta 1356):
    1️⃣ Pega o token
    2️⃣ Busca contrato (pega tenantid e subscriberId)
    3️⃣ Envia o POST para /beneficiaries/blockProtocol
    """

    try:
        # === 1️⃣ Obter token ===
        token = await medicar_get_token()

        # === 2️⃣ Buscar contrato (para pegar tenantid e subscriberId) ===
        contrato_data = await medicar_get_contract(token)
        tenantid = contrato_data.get("tenantid") or os.getenv("TENANT_ID")

        if not subscriber_id:
            subscriber_id = (
                contrato_data.get("subscriberId")
                or (contrato_data.get("bba") or {}).get("subscriberId")
            )

        if not tenantid:
            raise RuntimeError("❌ tenantid não encontrado no contrato da Medicar.")
        if not subscriber_id:
            raise RuntimeError("❌ subscriberId não encontrado no contrato da Medicar.")

        # === 3️⃣ Preparar requisição ===
        block_date = block_date or datetime.now().strftime("%Y-%m-%d")
        login_user = (login_user or os.getenv("MEDICAR_LOGIN_USER") or "api.atendemed")

        # ✅ Monta a URL a partir do .env
        MEDICAR_BASE_URL_1356 = os.getenv(
            "MEDICAR_BASE_URL_1356",
            "https://medicar146708.protheus.cloudtotvs.com.br:1356/rest"
        ).rstrip("/")

        url_block = f"{MEDICAR_BASE_URL_1356}/totvsHealthPlans/familyContract/v1/beneficiaries/blockProtocol"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json;charset=utf-8",
            "Accept": "application/json"
        }

        payload = {
            "subscriberId": str(subscriber_id),
            "reason": str(reason),
            "blockDate": block_date,
            "loginUser": only_ascii_upper(login_user)
        }

        params = {"tenantid": tenantid}

        log.info(f"🚀 Encerrando matrícula subscriberId={subscriber_id}, tenantid={tenantid}, blockDate={block_date}")

        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.post(url_block, headers=headers, params=params, json=payload)
            log.info(f"🔁 Resposta Medicar: {resp.status_code} -> {resp.text}")
            resp.raise_for_status()
            data = resp.json()

        log.info(f"✅ Matrícula encerrada com sucesso (subscriberId={subscriber_id})")
        return {
            "status": "ok",
            "tenantid": tenantid,
            "subscriberId": subscriber_id,
            "payload_enviado": payload,
            "resposta": data
        }

    except Exception as e:
        log.exception("Erro ao encerrar matrícula na Medicar")
        return {"status": "erro", "mensagem": str(e)}


# ============================================================
# ENDPOINT PRINCIPAL
# ============================================================
@app.post("/webhook/clientes")
async def webhook_clientes(request: Request):
    body = await request.json()
    items = body if isinstance(body, list) else [body]
    log.info(f"Webhook recebido: {items}")

    # autenticação Medicar
    try:
        token = await medicar_get_token()
    except Exception as e:
        log.exception("Erro ao autenticar na Medicar")
        return {"status": "erro", "mensagem": str(e)}

    # tenant
    tenantid = TENANT_ID
    if not tenantid:
        try:
            contr = await medicar_get_contract(token)
            tenantid = contr.get("tenantid")
        except Exception as e:
            log.warning(f"Não foi possível obter contrato padrão: {e}")

    contract_fields = json.loads(MEDICAR_CONTRACT_FIELDS_JSON) if MEDICAR_CONTRACT_FIELDS_JSON else None
    results = []

    for item in items:
        data = item.get("data", {}) if isinstance(item, dict) else {}
        if not data:
            continue

        cpf = data.get("cpf")
        id_cliente = data.get("id")

        try:
            # === 1️⃣ Buscar plano do titular (5 tentativas com delay de 60s) ===
            carteira = None
            for tentativa in range(5):
                carteira = await tenex_get_carteira(cpf)
                first = carteira[0] if isinstance(carteira, list) and carteira else None
                if first and first.get("planos_contratados"):
                    log.info(f"Plano encontrado na tentativa {tentativa+1} para CPF {cpf}")
                    break
                log.warning(f"Tentativa {tentativa+1}/5: plano não disponível para CPF {cpf}. Aguardando 60 s...")
                await asyncio.sleep(60)

            first = carteira[0] if isinstance(carteira, list) and carteira else None
            if not first or not first.get("planos_contratados"):
                results.append({
                    "cpf": cpf,
                    "status": "ignorado",
                    "motivo": "Nenhum plano encontrado após 5 tentativas"
                })
                continue

            pessoa = next((p for p in carteira if only_digits(p.get("cpf", "")) == only_digits(cpf)), first)
            id_plano = pessoa["planos_contratados"][0]["id_plano"]
            plano = PLAN_MAPPING_JSON.get(str(id_plano))
            if not plano:
                log.warning(f"Plano {id_plano} não mapeado — ignorando CPF {cpf}")
                results.append({"cpf": cpf, "status": "ignorado", "motivo": f"plano {id_plano} não mapeado"})
                continue

            # === 2️⃣ Buscar cliente + contatos (titular + dependentes) ===
            cliente_expand = await tenex_get_cliente_com_contatos(id_cliente)
            contatos = (cliente_expand or {}).get("contatos", []) if cliente_expand else []

            # titular: usa contatos[principal=1]; se não houver, usa o próprio data do webhook
            tit = next((c for c in contatos if str(c.get("principal")) == "1"), None)
            titular_dict = {
                "nome": only_ascii_upper((tit or data).get("nome") or ""),
                "cpf": only_digits((tit or data).get("cpf") or ""),
                "data_nascimento": ( (tit or data).get("data_nascimento") or "" ).replace("-", ""),
                "sexo": str((tit or data).get("genero") or "2"),
                "nome_mae": "NOME MAE NAO INFORMADO",
            }

            # dependentes
            dependentes_dicts = []
            for dep in contatos:
                if str(dep.get("principal")) != "0" or not dep.get("cpf"):
                    continue
                dependentes_dicts.append({
                    "nome": only_ascii_upper(dep.get("nome") or ""),
                    "cpf": only_digits(dep.get("cpf") or ""),
                    "data_nascimento": (dep.get("data_nascimento") or "").replace("-", ""),
                    "sexo": str(dep.get("genero") or "2"),
                    "nome_mae": "NOME MAE NAO INFORMADO",
                })

            # validações mínimas do titular
            if not titular_dict["nome"] or not titular_dict["cpf"]:
                results.append({"cpf": cpf, "status": "erro", "erro": "Titular sem nome/CPF válido"})
                continue

            # === 3️⃣ Enviar família (titular + dependentes) para a Medicar ===
            resp_medicar = await medicar_incluir_familia(
                token=token,
                tenantid=tenantid,
                titular=titular_dict,
                dependentes=dependentes_dicts,
                plano=plano,
                contract_fields=contract_fields,
            )

            log.info(f"✅ Família incluída: titular CPF={titular_dict['cpf']} (+{len(dependentes_dicts)} dep.)")
            results.append({"cpf": titular_dict["cpf"], "status": "cadastrado", "resposta": resp_medicar})

        except Exception as e:
            log.exception(f"Erro ao processar cliente {cpf}")
            results.append({"cpf": cpf, "status": "erro", "erro": str(e)})

    return {"status": "ok", "resultados": results}

@app.post("/webhook/encerrar")
async def webhook_encerrar(
    request: Request,
    subscriberId: str | None = None,
    reason: str = "000001",
    blockDate: str | None = None,      # "YYYY-MM-DD"
    loginUser: str | None = None
):
    """
    Teste manual do encerramento.
    Envie por query string OU body JSON:
    {
      "subscriberId": "10010004153123000",
      "reason": "000001",
      "blockDate": "2025-10-27",
      "loginUser": "ABEL DE CAMARGO"
    }
    Se 'subscriberId' não for enviado, será buscado no /client/v1/contract.
    """
    try:
        # tenta body JSON (opcional)
        try:
            body = await request.json()
        except Exception:
            body = {}

        subscriber_id = (subscriberId or body.get("subscriberId"))
        reason = (reason or body.get("reason") or "000001")
        block_date = (blockDate or body.get("blockDate"))  # se None, função usa hoje
        login_user = (loginUser or body.get("loginUser"))  # se None, usa MEDICAR_LOGIN_USER/env

        resultado = await medicar_encerrar_matricula(
            subscriber_id=subscriber_id,
            reason=reason,
            block_date=block_date,
            login_user=login_user
        )
        return resultado

    except Exception as e:
        log.exception("Erro no /webhook/encerrar")
        return {"status": "erro", "mensagem": str(e)}


@app.get("/health")
async def health():
    return {"status": "online", "servico": "TENEX → MEDICAR (async)"}


