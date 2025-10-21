from fastapi import FastAPI, Request
import requests, os, json, time, logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

app = FastAPI(title="Atende Med – Integração TENEX → MEDICAR")

# Configuração de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("atendmed-api")

# ==============================
# Utilitários
# ==============================

def only_digits(s: str) -> str:
    return "".join(ch for ch in s if ch.isdigit())

@retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=6),
       retry=retry_if_exception_type(requests.RequestException))
def request_retry(method: str, url: str, **kwargs):
    resp = requests.request(method, url, timeout=20, **kwargs)
    resp.raise_for_status()
    return resp

# ==============================
# Variáveis de ambiente
# ==============================

TENEX_BASE_URL = os.getenv("TENEX_BASE_URL", "https://maisaudebh.tenex.com.br")
TENEX_BASIC_AUTH = os.getenv("TENEX_BASIC_AUTH")

MEDICAR_BASE_URL = os.getenv("MEDICAR_BASE_URL")
MEDICAR_USERNAME = os.getenv("MEDICAR_USERNAME")
MEDICAR_PASSWORD = os.getenv("MEDICAR_PASSWORD")
MEDICAR_CNPJMEDICAR = "".join(filter(str.isdigit, os.getenv("MEDICAR_CNPJMEDICAR", "")))
MEDICAR_GRUPOEMPRESA = os.getenv("MEDICAR_GRUPOEMPRESA")
MEDICAR_CONTRATO = os.getenv("MEDICAR_CONTRATO")

PLAN_MAPPING_JSON = json.loads(os.getenv(
    "PLAN_MAPPING_JSON",
    '{"34":{"codpro":"0066","versao":"001"},"35":{"codpro":"0066","versao":"001"}}'
))

# ==============================
# Funções auxiliares
# ==============================

def get_tenex_carteira(cpf: str):
    url = f"{TENEX_BASE_URL}/api/v2/carteira-virtual/{only_digits(cpf)}"
    headers = {"Authorization": f"Basic {TENEX_BASIC_AUTH}"}
    resp = request_retry("GET", url, headers=headers)
    return resp.json()

def get_medicar_token():
    url = f"{MEDICAR_BASE_URL}/api/oauth2/v1/token"
    payload = {
        "grant_type": "password",
        "username": MEDICAR_USERNAME,
        "password": MEDICAR_PASSWORD
    }
    headers = {"Content-Type": "application/json"}
    resp = request_retry("POST", url, headers=headers, json=payload)

    if resp.status_code != 200:
        log.error(f"Falha ao obter token (HTTP {resp.status_code}): {resp.text}")
        resp.raise_for_status()

    token_data = resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise ValueError(f"Resposta inválida ao obter token: {token_data}")

    log.info("✅ Token da Medicar obtido com sucesso.")
    return access_token

def get_medicar_contract(token: str, cpf: str = None):
    url = f"{MEDICAR_BASE_URL}/client/v1/contract"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "cnpjmedicar": MEDICAR_CNPJMEDICAR,
        "grupoempresa": MEDICAR_GRUPOEMPRESA,
        "contrato": MEDICAR_CONTRATO
    }
    if cpf:
        params["cgcbeneficiario"] = only_digits(cpf)
    resp = request_retry("GET", url, headers=headers, params=params)
    return resp.json()

def incluir_beneficiario(token: str, tenantid: str, nome: str, cpf: str, data_nasc: str, sexo: int, plano: dict):
    url = f"{MEDICAR_BASE_URL}/fwmodel/PLIncBenModel/"
    headers = {
        "Authorization": f"Bearer {token}",
        "tenantid": tenantid,
        "Content-Type": "application/json"
    }

    dt_nasc_fmt = data_nasc.replace("-", "")
    sexo_valor = "1" if sexo == 1 else "2"

    payload = {
        "id": "PLIncBenModel",
        "operation": 4,
        "models": [
            {
                "id": "MASTERBBA",
                "modeltype": "FIELDS",
                "fields": [
                    {"id": "BBA_EMPBEN", "value": nome},
                    {"id": "BBA_CODPRO", "value": plano["codpro"]},
                    {"id": "BBA_VERSAO", "value": plano["versao"]},
                    {"id": "BBA_CPFTIT", "value": only_digits(cpf)},
                ],
                "models": [
                    {
                        "id": "DETAILB2N",
                        "modeltype": "GRID",
                        "items": [
                            {
                                "id": 1,
                                "deleted": 0,
                                "fields": [
                                    {"id": "B2N_NOMUSR", "value": nome},
                                    {"id": "B2N_DATNAS", "value": dt_nasc_fmt},
                                    {"id": "B2N_SEXO", "value": sexo_valor},
                                    {"id": "B2N_CODPRO", "value": plano["codpro"]},
                                    {"id": "B2N_CPFUSR", "value": only_digits(cpf)},
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }

    resp = request_retry("POST", url, headers=headers, json=payload)
    return resp.json()

# ==============================
# ENDPOINTS
# ==============================

@app.post("/webhook/clientes")
async def webhook_clientes(request: Request):
    body = await request.json()
    log.info(f"Webhook recebido: {body}")
    results = []

    try:
        token = get_medicar_token()
        contract = get_medicar_contract(token)
        tenantid = contract.get("tenantid")
    except Exception as e:
        log.exception("❌ Erro ao autenticar na Medicar")
        return {"status": "erro", "mensagem": f"Falha na autenticação com a Medicar: {e}"}

    for item in body:
        data = item.get("data", {})
        if not data:
            continue

        nome = data.get("nome")
        cpf = data.get("cpf")
        data_nasc = data.get("data_nascimento")
        sexo = data.get("genero")

        try:
            # Tenta buscar plano 5x com delay de 1 min
            carteira = []
            for tentativa in range(5):
                carteira = get_tenex_carteira(cpf)
                if carteira and carteira[0].get("planos_contratados"):
                    log.info(f"Plano encontrado na tentativa {tentativa+1} para CPF {cpf}")
                    break
                log.warning(f"Tentativa {tentativa+1}/5: plano ainda não disponível para CPF {cpf}. Aguardando 60s...")
                time.sleep(60)

            if not carteira or not carteira[0].get("planos_contratados"):
                results.append({"cpf": cpf, "status": "ignorado", "motivo": "Nenhum plano encontrado após 5 tentativas"})
                continue

            pessoa = next((p for p in carteira if only_digits(p.get("cpf", "")) == only_digits(cpf)), carteira[0])
            id_plano = pessoa["planos_contratados"][0]["id_plano"]
            plano = PLAN_MAPPING_JSON.get(str(id_plano))

            if not plano:
                results.append({"cpf": cpf, "status": "ignorado", "motivo": f"plano {id_plano} não mapeado"})
                continue

            resp_medicar = incluir_beneficiario(token, tenantid, nome, cpf, data_nasc, sexo, plano)
            log.info(f"✅ Cliente cadastrado com sucesso CPF={cpf}")
            results.append({"cpf": cpf, "status": "cadastrado", "resposta": resp_medicar})

        except Exception as e:
            log.exception(f"❌ Erro ao processar cliente {cpf}")
            results.append({"cpf": cpf, "status": "erro", "erro": str(e)})

    return {"status": "ok", "resultados": results}


@app.get("/health")
def health():
    return {"status": "online", "servico": "TENEX → MEDICAR"}
