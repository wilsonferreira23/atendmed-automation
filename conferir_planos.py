"""
conferir_planos.py
------------------
Lê o arquivo downgrades_consolidado.txt e verifica, para cada CPF,
se a pessoa AINDA possui o plano 31 ou 32 ATIVO na carteira-virtual do Tenex.

Resultado será salvo em: conferencia_resultado.txt
"""

import os
import re
import time
import requests
from datetime import datetime

# ──────────────────────────────────────────
# CONFIG – adapte se precisar
# ──────────────────────────────────────────
TENEX_BASE_URL   = os.getenv("TENEX_BASE_URL", "https://maisaudebh.tenex.com.br").rstrip("/")
TENEX_BASIC_AUTH = os.getenv("TENEX_BASIC_AUTH", "MDRzTzdYUjdjVGlmRW12ZDFWcXNybGhKd1BPNXNlY3hiS0oxQmtHcDJvYzo=")          # token Base64 já codificado

# Planos que qualificam para a Medicar
PLANOS_QUALIFICADOS = {"31", "32"}

INPUT_FILE  = "downgrades_consolidado.txt"
OUTPUT_FILE = "conferencia_resultado.txt"

# ──────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────
def only_digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def checar_carteira(cpf: str) -> dict:
    """
    Consulta a carteira-virtual do Tenex para o CPF.
    Retorna dict com:
      - plano_ativo: bool
      - planos_encontrados: list[int]   ← todos os id_plano com status ativo
      - planos_inativos: list[int]      ← id_plano com status False
      - erro: str | None
    """
    url = f"{TENEX_BASE_URL}/api/v2/carteira-virtual/{cpf}"
    headers = {"Authorization": f"Basic {TENEX_BASIC_AUTH}"}

    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {"plano_ativo": False, "planos_encontrados": [], "planos_inativos": [], "erro": str(e)}

    if not isinstance(data, list) or not data:
        return {"plano_ativo": False, "planos_encontrados": [], "planos_inativos": [], "erro": "Resposta vazia"}

    # pega a entrada do próprio CPF (titular ou dependente)
    pessoa = next((p for p in data if only_digits(p.get("cpf", "")) == cpf), data[0])

    ativos   = []
    inativos = []
    for p in pessoa.get("planos_contratados", []):
        pid = p.get("id_plano")
        if pid is None:
            continue
        if p.get("carteira_virtual_status"):
            ativos.append(str(pid))
        else:
            inativos.append(str(pid))

    plano_ativo = any(pid in PLANOS_QUALIFICADOS for pid in ativos)
    return {
        "plano_ativo": plano_ativo,
        "planos_encontrados": ativos,
        "planos_inativos": inativos,
        "erro": None
    }


# ──────────────────────────────────────────
# LEITURA DO TXT
# ──────────────────────────────────────────
def ler_lista(caminho: str) -> list[tuple[str, str]]:
    """Retorna lista de (nome, cpf_digits)."""
    resultado = []
    with open(caminho, encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            if not linha:
                continue
            if ":" in linha:
                nome, cpf_raw = linha.split(":", 1)
                resultado.append((nome.strip(), only_digits(cpf_raw.strip())))
    return resultado


# ──────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(" CONFERÊNCIA DE PLANOS – TENEX CARTEIRA VIRTUAL")
    print(f" {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'='*60}\n")

    pessoas = ler_lista(INPUT_FILE)
    total   = len(pessoas)
    print(f"📋 Total de CPFs para conferir: {total}\n")

    com_plano    = []   # ainda têm plano 31/32 ativo
    sem_plano    = []   # confirmados sem plano qualificado
    com_erro     = []   # erro na consulta

    for i, (nome, cpf) in enumerate(pessoas, 1):
        print(f"[{i:02d}/{total}] {nome} ({cpf})...", end=" ", flush=True)

        resultado = checar_carteira(cpf)

        if resultado["erro"]:
            print(f"❌ ERRO: {resultado['erro']}")
            com_erro.append((nome, cpf, resultado["erro"]))
        elif resultado["plano_ativo"]:
            ativos_qual = [p for p in resultado["planos_encontrados"] if p in PLANOS_QUALIFICADOS]
            print(f"⚠️  AINDA TEM PLANO ATIVO! Planos: {ativos_qual}")
            com_plano.append((nome, cpf, ativos_qual))
        else:
            todos_inativos = resultado["planos_inativos"] + resultado["planos_encontrados"]
            print(f"✅ Sem plano qualificado (planos inativos/outros: {todos_inativos})")
            sem_plano.append((nome, cpf, todos_inativos))

        time.sleep(0.3)   # respeita rate limit do Tenex

    # ──────────────────────────────────────────
    # RELATÓRIO
    # ──────────────────────────────────────────
    linhas = []
    linhas.append(f"CONFERÊNCIA DE PLANOS – {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    linhas.append(f"Total consultado: {total}")
    linhas.append(f"Sem plano qualificado (OK): {len(sem_plano)}")
    linhas.append(f"AINDA com plano 31/32 ativo: {len(com_plano)}")
    linhas.append(f"Erros de consulta: {len(com_erro)}")
    linhas.append("")

    if com_plano:
        linhas.append("=" * 60)
        linhas.append("⚠️  ATENÇÃO – AINDA POSSUEM PLANO 31 OU 32 ATIVO:")
        linhas.append("=" * 60)
        for nome, cpf, planos in com_plano:
            linhas.append(f"{nome}: {cpf}  (planos: {', '.join(planos)})")
        linhas.append("")

    linhas.append("=" * 60)
    linhas.append("✅  CONFIRMADOS SEM PLANO QUALIFICADO:")
    linhas.append("=" * 60)
    for nome, cpf, outros in sem_plano:
        outros_str = f"(outros planos: {', '.join(outros)})" if outros else "(nenhum plano)"
        linhas.append(f"{nome}: {cpf}  {outros_str}")
    linhas.append("")

    if com_erro:
        linhas.append("=" * 60)
        linhas.append("❌  ERROS DE CONSULTA:")
        linhas.append("=" * 60)
        for nome, cpf, erro in com_erro:
            linhas.append(f"{nome}: {cpf}  → {erro}")

    relatorio = "\n".join(linhas)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(relatorio)

    print(f"\n{'='*60}")
    print(f"Relatório salvo em: {OUTPUT_FILE}")
    print(f"  ✅ Sem plano qualificado: {len(sem_plano)}")
    print(f"  ⚠️  Ainda com plano 31/32: {len(com_plano)}")
    print(f"  ❌ Erros: {len(com_erro)}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
