from fastapi import FastAPI, Request
import os, json, logging, asyncio
import httpx

app = FastAPI(title="Atende Med – Integração TENEX → MEDICAR (async httpx)")

# ============================================================
# LOGS
# ============================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("atendmed-api")


# ============================================================
# FUNÇÕES AUXILIARES E CONFIG
# ============================================================

def only_digits(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())


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


# ============================================================
# CLIENTES HTTP COM RETENTATIVA
# ============================================================

async def httpx_retry(method: str, url: str, **kwargs) -> httpx.Response:
    tries = 3
    delay = 1.0
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        for i in range(tries):
            try:
                resp = await client.request(method, url, **kwargs)
                resp.raise_for_status()
                return resp
            except httpx.HTTPError as e:
                log.warning(f"Tentativa {i+1}/{tries} falhou para {url}: {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 6)
        raise RuntimeError(f"Falha após {tries} tentativas para {url}")


# ============================================================
# TENEX → Obter plano por CPF
# ============================================================

async def tenex_get_carteira(cpf: str):
    url = f"{TENEX_BASE_URL}/api/v2/carteira-virtual/{only_digits(cpf)}"
    headers = {"Authorization": f"Basic {TENEX_BASIC_AUTH}"}
    resp = await httpx_retry("GET", url, headers=headers)
    return resp.json()


# ============================================================
# MEDICAR → Autenticação
# ============================================================

async def medicar_get_token():
    url = f"{MEDICAR_BASE_URL}/api/oauth2/v1/token"
    params = {
        "grant_type": "password",
        "username": MEDICAR_USERNAME,
        "password": MEDICAR_PASSWORD,
    }
    headers = {"Content-Type": "application/json"}
    json_body = {"name": "API-Integration-Client"}
    resp = await httpx_retry("POST", url, params=params, json=json_body, headers=headers)
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"Token inválido: {data}")
    log.info("✅ Token da Medicar obtido com sucesso.")
    return token, data.get("expires_in")


# ============================================================
# MEDICAR → Inclusão de beneficiário
# ============================================================

async def medicar_incluir_beneficiario(token, tenantid, nome, cpf, data_nasc_iso, sexo_int, plano, contract_fields):
    url = f"{MEDICAR_BASE_URL}/fwmodel/PLIncBenModel/"
    params = {"tenantId": tenantid}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    dt_nasc = (data_nasc_iso or "").replace("-", "")
    sexo_valor = "1" if sexo_int == 1 else "2"

    fields_master = []
    if contract_fields:
        order = 1
        for key in ["BBA_CODINT","BBA_CODEMP","BBA_CONEMP","BBA_VERCON","BBA_SUBCON","BBA_VERSUB"]:
            if contract_fields.get(key):
                fields_master.append({"id": key, "order": order, "value": contract_fields[key]})
                order += 1

    fields_master.extend([
        {"id": "BBA_EMPBEN", "value": nome},
        {"id": "BBA_CODPRO", "value": plano["codpro"]},
        {"id": "BBA_VERSAO", "value": plano["versao"]},
        {"id": "BBA_CPFTIT", "value": only_digits(cpf)},
    ])

    detail_b2n_items = [{
        "id": 1,
        "deleted": 0,
        "fields": [
            {"id": "B2N_NOMUSR", "value": nome},
            {"id": "B2N_DATNAS", "value": dt_nasc},
            {"id": "B2N_GRAUPA", "value": "00"},
            {"id": "B2N_ESTCIV", "value": "S"},
            {"id": "B2N_SEXO", "value": sexo_valor},
            {"id": "B2N_CPFUSR", "value": only_digits(cpf)},
            {"id": "B2N_CODPRO", "value": plano["codpro"]},
        ]
    }]

    payload = {
        "id": "PLIncBenModel",
        "operation": 3,
        "models": [
            {
                "id": "MASTERBBA",
                "modeltype": "FIELDS",
                "fields": fields_master,
                "models": [
                    {"id": "DETAILB2N", "modeltype": "GRID", "items": detail_b2n_items},
                    {"id": "DETAILANEXO", "modeltype": "GRID", "items": [{"id": 1, "deleted": 0, "fields": []}]},
                ],
            }
        ],
    }

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.post(url, params=params, headers=headers, json=payload)
        resp.raise_for_status()
        return resp.json()


# ============================================================
# ENDPOINT PRINCIPAL /webhook/clientes
# ============================================================

@app.post("/webhook/clientes")
async def webhook_clientes(request: Request):
    body = await request.json()
    items = body if isinstance(body, list) else [body]
    log.info(f"Webhook recebido: {items}")

    try:
        token, _ = await medicar_get_token()
    except Exception as e:
        log.exception("Erro ao autenticar na Medicar")
        return {"status": "erro", "mensagem": str(e)}

    tenantid = TENANT_ID
    contract_fields = json.loads(MEDICAR_CONTRACT_FIELDS_JSON) if MEDICAR_CONTRACT_FIELDS_JSON else None

    results = []
    for item in items:
        data = item.get("data", {})
        nome = data.get("nome")
        cpf = data.get("cpf")
        data_nasc = data.get("data_nascimento")
        sexo = data.get("genero")

        try:
            carteira = None
            for tentativa in range(5):
                carteira = await tenex_get_carteira(cpf)
                first = carteira[0] if isinstance(carteira, list) and carteira else None
                if first and first.get("planos_contratados"):
                    break
                log.warning(f"Tentativa {tentativa+1}/5: aguardando plano Tenex...")
                await asyncio.sleep(60)

            if not carteira or not carteira[0].get("planos_contratados"):
                results.append({"cpf": cpf, "status": "ignorado", "motivo": "Plano não encontrado"})
                continue

            pessoa = next((p for p in carteira if only_digits(p.get("cpf", "")) == only_digits(cpf)), carteira[0])
            id_plano = pessoa["planos_contratados"][0]["id_plano"]
            plano = PLAN_MAPPING_JSON.get(str(id_plano))
            if not plano:
                results.append({"cpf": cpf, "status": "ignorado", "motivo": f"plano {id_plano} não mapeado"})
                continue

            resp_medicar = await medicar_incluir_beneficiario(
                token, tenantid, nome, cpf, data_nasc, sexo, plano, contract_fields
            )
            log.info(f"✅ Cliente cadastrado com sucesso CPF={cpf}")
            results.append({"cpf": cpf, "status": "cadastrado", "resposta": resp_medicar})

        except Exception as e:
            log.exception(f"Erro ao processar cliente {cpf}")
            results.append({"cpf": cpf, "status": "erro", "erro": str(e)})

    return {"status": "ok", "resultados": results}


@app.get("/health")
async def health():
    return {"status": "online", "servico": "TENEX → MEDICAR"}

