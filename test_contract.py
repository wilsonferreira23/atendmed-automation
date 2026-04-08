import asyncio
from app import medicar_get_token, httpx_retry
from dotenv import load_dotenv
import os

load_dotenv()

async def main():
    token = await medicar_get_token()
    print("Token fetched")
    
    url_mat = f"{os.getenv('MEDICAR_BASE_URL')}/client/v1/contract"
    headers = {"Authorization": f"Bearer {token}"}
    for cpf in ["51805807064", "41780322089", "01587395070", "88944352020"]:
        params_mat = {
            "cnpjmedicar": os.getenv("MEDICAR_CNPJMEDICAR"),
            "grupoempresa": os.getenv("MEDICAR_GRUPOEMPRESA"),
            "contrato": os.getenv("MEDICAR_CONTRATO"),
            "cgcbeneficiario": cpf,
        }
        resp = await httpx_retry("GET", url_mat, headers=headers, params=params_mat)
        print(f"--- CPF {cpf} ---")
        print("Status:", resp.status_code)
        try:
            print("JSON:", resp.json())
        except:
            print("Text:", resp.text)

asyncio.run(main())
