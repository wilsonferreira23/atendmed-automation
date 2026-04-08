import asyncio
from app import medicar_get_token, medicar_get_contract, PLAN_MAPPING_JSON
from app import HTTP_TIMEOUT, MEDICAR_BASE_URL
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

async def main():
    token = await medicar_get_token()
    base_contract = await medicar_get_contract(token)
    plano = PLAN_MAPPING_JSON.get("32") or {"codpro": "0066", "versao": "001"}
    
    cpf = "69074401058"
    nome = "TESTE MEDICAR ONZE"
    
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

    items = [{
        "id": 1,
        "deleted": 0,
        "fields": [
            {"id": "B2N_NOMUSR", "value": nome},
            {"id": "B2N_DATNAS", "value": "19950505"},
            {"id": "B2N_GRAUPA", "value": "00"},
            {"id": "B2N_ESTCIV", "value": "S"},
            {"id": "B2N_SEXO", "value": "1"},
            {"id": "B2N_CPFUSR", "value": cpf},
            {"id": "B2N_MAE", "value": "NOME MAE NAO INFORMADO"},
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

    url = f"{MEDICAR_BASE_URL}/fwmodel/PLIncBenModel/"
    params = {"tenantId": "01,006001"}
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.post(url, params=params, headers=headers, json=payload)
    print("STATUS CREACAO:", resp.status_code)
    try:
        j = resp.json()
        print("TEM BBA_MATRIC NO RETORNO?")
        found = False
        for model in j.get("models", []):
            if model.get("id") == "MASTERBBA":
                for field in model.get("fields", []):
                    if field.get("id") == "BBA_MATRIC":
                        print("BBA_MATRIC VALUE:", field.get("value"))
                        found = True
        if not found:
            print("FULL RESPONSE:")
            import json
            print(json.dumps(j, indent=2))
    except Exception as e:
        print("Exception:", e)

asyncio.run(main())
