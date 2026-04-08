import asyncio
from app import medicar_get_token, medicar_incluir_titular, medicar_get_contract, PLAN_MAPPING_JSON
from app import HTTP_TIMEOUT
import os
from dotenv import load_dotenv

load_dotenv()

async def main():
    token = await medicar_get_token()
    base_contract = await medicar_get_contract(token)
    plano = PLAN_MAPPING_JSON.get("31") or {"codpro": "0066", "versao": "001"}
    
    cpf = "68212133095" # NOVO CPF TESTE
    nome = "TESTE MEDICAR ONZE"
    
    titular = {
        "cpf": cpf,
        "nome": nome,
        "data_nascimento": "19950505",
        "sexo": "1",
        "nome_mae": "NOME MAE",
    }
    
    tenantid = "01,006001"
    contract_fields = base_contract
    
    print("Enviando POST...")
    resp = await medicar_incluir_titular(token, tenantid, titular, plano, contract_fields)
    print("STATUS CREACAO (nao vazou erro)")
    import json
    print("JSON RETORNADO:")
    print(json.dumps(resp, indent=2))
    
    url_mat = f"{os.getenv('MEDICAR_BASE_URL')}/client/v1/contract"
    headers = {"Authorization": f"Bearer {token}"}
    params_mat = {
        "cnpjmedicar": os.getenv("MEDICAR_CNPJMEDICAR"),
        "grupoempresa": os.getenv("MEDICAR_GRUPOEMPRESA"),
        "contrato": os.getenv("MEDICAR_CONTRATO"),
        "cgcbeneficiario": cpf,
    }
    import httpx
    async with httpx.AsyncClient() as client:
        r2 = await client.get(url_mat, headers=headers, params=params_mat)
        print("GET API RESPONSE:", r2.status_code)
        print("GET", r2.json())

asyncio.run(main())
