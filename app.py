from fastapi import FastAPI, Request
from pydantic import BaseModel
import os, json, logging, asyncio, time
import httpx
from datetime import datetime, timedelta

app = FastAPI(title="Atende Med – Integração TENEX → MEDICAR (async)")

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

TENANT_ID = os.getenv("TENANT_ID")  # ex.: "01,006001"
# Ex.: {"BBA_CODINT":"1001","BBA_CODEMP":"0004","BBA_CONEMP":"000000000002","BBA_VERCON":"001","BBA_SUBCON":"002326875","BBA_VERSUB":"001"}
MEDICAR_CONTRACT_FIELDS_JSON = os.getenv("MEDICAR_CONTRACT_FIELDS_JSON", "")

PLAN_MAPPING_JSON = json.loads(os.getenv(
    "PLAN_MAPPING_JSON",
    '{"34":{"codpro":"0066","versao":"001"},"35":{"codpro":"0066","versao":"001"}}'
))

HTTP_TIMEOUT = 25.0

# cache simples de token
_token_cache = {"token": None, "expiry": datetime.min}

# ============================================================
# CLIENTE HTTP COM RETENTATIVA
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
# TENEX
# ============================================================

async def tenex_get_carteira(cpf: str):
    url = f"{TENEX_BASE_URL}/api/v2/carteira-virtual/{only_digits(cpf)}"
    headers = {"Authorization": f"Basic {TENEX_BASIC_AUTH}"}
    resp = await httpx_retry("GET", url, headers=headers)
    return resp.json()

# ============================================================
# MEDICAR – TOKEN, CONTRATO, INCLUSÃO
# ============================================================

async def medicar_get_token():
    # cache
    if _token_cache["token"] and datetime.now() < _token_cache["expiry"]:
        return _token_cache["token"]

    url = f"{MEDICAR_BASE_URL}/api/oauth2/v1/token"
    params = {"grant_type": "password", "username": MEDICAR_USERNAME, "password": MEDICAR_PASSWORD}
    headers = {"Content-Type": "application/json"}
    json_body = {"name": "API-Integration-Client"}  # compat c/ exemplo funcional
    resp = await httpx_retry("POST", url, params=params, json=json_body, headers=headers)

    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"Token inválido: {data}")

    # expiração (menos 60s de margem)
    ttl = int(data.get("expires_in", 3600)) - 60
    _token_cache["token"] = token
    _token_cache["expiry"] = datetime.now() + timedelta(seconds=max(ttl, 60))

    log.info("✅ Token da Medicar obtido com sucesso.")
    return token

async def medicar_get_contract(token: str, cpf: str | None = None):
    url = f"{MEDICAR_BASE_URL}/client/v1/contract"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"cnpjmedicar": MEDICAR_CNPJMEDICAR, "grupoempresa": MEDICAR_GRUPOEMPRESA, "contrato": MEDICAR_CONTRATO}
    if cpf:
        params["cgcbeneficiario"] = only_digits(cpf)
    resp = await httpx_retry("GET", url, headers=headers, params=params)
    return resp.json()

async def medicar_incluir_beneficiario(token, tenantid, nome, cpf, data_nasc_iso, sexo_int, plano, contract_fields):
    url = f"{MEDICAR_BASE_URL}/fwmodel/PLIncBenModel/"
    params = {"tenantId": tenantid}  # como no seu exemplo
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    dt_nasc = (data_nasc_iso or "").replace("-", "")
    sexo_valor = "1" if str(sexo_int) == "1" else "2"

    fields_master = []
    # Se vierem campos do contrato fixos por env, usa-os
    if contract_fields:
        order = 1
        for key in ["BBA_CODINT","BBA_CODEMP","BBA_CONEMP","BBA_VERCON","BBA_SUBCON","BBA_VERSUB"]:
            if contract_fields.get(key):
                fields_master.append({"id": key, "order": order, "value": contract_fields[key]})
                order += 1

    # Campos mínimos do titular
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
            {"id": "B2N_GRAUPA", "value": "00"},  # titular
            {"id": "B2N_ESTCIV", "value": "S"},
            {"id": "B2N_SEXO", "value": sexo_valor},
            {"id": "B2N_CPFUSR", "value": only_digits(cpf)},
            {"id": "B2N_CODPRO", "value": plano["codpro"]},
        ]
    }]

    payload = {
        "id": "PLIncBenModel",
        "operation": 3,  # conforme exemplo funcional
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
# MODELO DO TESTE
# ============================================================

class PessoaTeste(BaseModel):
    nome: str
    cpf: str                       # pode vir com ou sem pontuação
    data_nascimento: str           # AAAAMMDD ou AAAA-MM-DD (ambos aceitos)
    sexo: str                      # "1" masc, "2" fem
    nome_mae: str = "sem informação"
    estado_civil_cod: str = "S"    # Solteiro (padrão)
    codpro: str | None = None      # opcional: sobrescrever plano
    versao: str | None = None      # opcional: sobrescrever versão

# ============================================================
# ENDPOINT DE TESTE (DADOS NO CORPO)
# ============================================================

@app.post("/teste-medicar")
async def teste_medicar(pessoa: PessoaTeste):
    try:
        token = await medicar_get_token()
        tenantid = TENANT_ID
        if not tenantid:
            # tenta buscar automaticamente
            contr = await medicar_get_contract(token)
            tenantid = contr.get("tenantid")
            if not tenantid:
                raise RuntimeError("tenantId não identificado (defina TENANT_ID ou confirme via /contract).")

        contract_fields = json.loads(MEDICAR_CONTRACT_FIELDS_JSON) if MEDICAR_CONTRACT_FIELDS_JSON else None

        # Plano: usa o que vier no body; se não vier, usa defaults 0066/001
        plano = {
            "codpro": pessoa.codpro or "0066",
            "versao": pessoa.versao or "001",
        }

        # aceita AAAA-MM-DD ou AAAAMMDD
        dn = pessoa.data_nascimento
        dn_fmt = dn if len(dn) == 8 and dn.isdigit() else dn.replace("-", "")

        resp_medicar = await medicar_incluir_beneficiario(
            token=token,
            tenantid=tenantid,
            nome=pessoa.nome,
            cpf=pessoa.cpf,
            data_nasc_iso=dn_fmt,
            sexo_int=int(pessoa.sexo),
            plano=plano,
            contract_fields=contract_fields,
        )
        return {"status": "ok", "mensagem": f"Beneficiário {pessoa.nome} enviado com sucesso!", "resposta": resp_medicar}

    except httpx.HTTPStatusError as e:
        return {"status": "erro", "detalhe": e.response.text}
    except Exception as e:
        log.exception("Erro no /teste-medicar")
        return {"status": "erro", "detalhe": str(e)}

# ============================================================
# ENDPOINT PRINCIPAL (WEBHOOK TENEX)
# ============================================================

@app.post("/webhook/clientes")
async def webhook_clientes(request: Request):
    body = await request.json()
    items = body if isinstance(body, list) else [body]
    log.info(f"Webhook recebido: {items}")

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

        nome = data.get("nome")
        cpf = data.get("cpf")
        data_nasc = data.get("data_nascimento")
        sexo = data.get("genero")

        try:
            # 5 tentativas, 1 min entre cada, para aguardar plano na Tenex
            carteira = None
            for tentativa in range(5):
                carteira = await tenex_get_carteira(cpf)
                first = carteira[0] if isinstance(carteira, list) and carteira else None
                if first and first.get("planos_contratados"):
                    log.info(f"Plano encontrado na tentativa {tentativa+1} para CPF {cpf}")
                    break
                log.warning(f"Tentativa {tentativa+1}/5: plano não disponível para CPF {cpf}. Aguardando 60s...")
                await asyncio.sleep(60)

            first = carteira[0] if isinstance(carteira, list) and carteira else None
            if not first or not first.get("planos_contratados"):
                results.append({"cpf": cpf, "status": "ignorado", "motivo": "Nenhum plano encontrado após 5 tentativas"})
                continue

            # seleciona registro com mesmo CPF se existir
            pessoa = next((p for p in carteira if only_digits(p.get("cpf","")) == only_digits(cpf)), first)
            id_plano = pessoa["planos_contratados"][0]["id_plano"]
            plano = PLAN_MAPPING_JSON.get(str(id_plano))
            if not plano:
                results.append({"cpf": cpf, "status": "ignorado", "motivo": f"plano {id_plano} não mapeado"})
                continue

            if not tenantid:
                raise RuntimeError("tenantId não identificado (defina TENANT_ID ou habilite leitura via /contract).")

            # aceita AAAA-MM-DD ou AAAAMMDD
            dn_fmt = data_nasc if (data_nasc and len(data_nasc) == 8 and data_nasc.isdigit()) else (data_nasc or "").replace("-", "")

            resp_medicar = await medicar_incluir_beneficiario(
                token=token,
                tenantid=tenantid,
                nome=nome,
                cpf=cpf,
                data_nasc_iso=dn_fmt,
                sexo_int=int(sexo) if sexo is not None else 1,
                plano=plano,
                contract_fields=contract_fields,
            )
            log.info(f"✅ Cliente cadastrado com sucesso CPF={cpf}")
            results.append({"cpf": cpf, "status": "cadastrado", "resposta": resp_medicar})

        except Exception as e:
            log.exception(f"Erro ao processar cliente {cpf}")
            results.append({"cpf": cpf, "status": "erro", "erro": str(e)})

    return {"status": "ok", "resultados": results}

# ============================================================
# HEALTHCHECK
# ============================================================

@app.get("/health")
async def health():
    return {"status": "online", "servico": "TENEX → MEDICAR (async)"}



@app.get("/health")
async def health():
    return {"status": "online", "servico": "TENEX → MEDICAR"}

