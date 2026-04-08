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
    
    cpf = "88944352020" # Vou usar o Teste 5 que nao foi criado
    nome = "TESTE MEDICAR ONZE"
    
    titular = {
        "cpf": cpf,
        "nome": nome,
        "data_nascimento": "19950505",
        "sexo": "1",
        "nome_mae": "NOME MAE NAO INFORMADO",
    }
    
    tenantid = "01,006001"
    contract_fields = base_contract
    
    print("Enviando POST...")
    resp = await medicar_incluir_titular(token, tenantid, titular, plano, contract_fields)
    print("STATUS CREACAO (nao vazou erro)")
    import json
    print("JSON RETORNADO:")
    print(json.dumps(resp, indent=2))

asyncio.run(main())
