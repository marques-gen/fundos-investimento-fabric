# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fc7d1418-362b-4352-9fd4-9ef4c5026821",
# META       "default_lakehouse_name": "lakehouse_bronze",
# META       "default_lakehouse_workspace_id": "61df9dee-1bf7-4985-975b-82a6be49a59a",
# META       "known_lakehouses": [
# META         {
# META           "id": "fc7d1418-362b-4352-9fd4-9ef4c5026821"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# =====================================================================
#  Imports bibliotecas necessárias
# ======================================================================

import os
import requests
import pandas as pd
import re
import time
import random
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
import logging
from typing import List

# Definindo cabeçalho para evitar bloqueio por scraping
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# URL base do portal da CVM
BASE_URL = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"

# Pasta local temporária (no Fabric Notebook)
#LOCAL_TEMP = "/lakehouse/default/Files/raw/inf_diario_fi_temp"

# Pasta final no Lakehouse - RAW
LAKEHOUSE_RAW_PATH = "/lakehouse/default/Files/landing/inf_diario_fi"

# Cria diretórios se não existirem
#Path(LOCAL_TEMP).mkdir(parents=True, exist_ok=True)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# listar arquivos e metadados no portal da CVM
# =======================================================================

def listar_arquivos_portal_cvm():
    response = requests.get(BASE_URL, headers=HEADERS)
    soup = BeautifulSoup(response.content, "html.parser")
    
    arquivos = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.endswith(".zip"):
            nome_arquivo = href
            url_completa = BASE_URL + href

            # Busca data de modificação
            try:
                head_resp = requests.head(url_completa, headers=HEADERS)
                modificado_em = head_resp.headers.get("Last-Modified", None)
                data_mod = datetime.strptime(modificado_em, "%a, %d %b %Y %H:%M:%S %Z") if modificado_em else None
            except:
                data_mod = None
            
            arquivos.append({
                "arquivo": nome_arquivo,
                "url": url_completa,
                "data_modificacao": data_mod
            })
            
            # time.sleep(random.uniform(0.5, 1.5))  # Evita bloqueios
            time.sleep(random.uniform(1, 2))  # Evita bloqueios 5 10
            
    return pd.DataFrame(arquivos)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==================================================================
# Lista arquivos do lakehouse
# ==================================================================
from pathlib import Path

def listar_arquivos_lakehouse():
    arquivos_existentes = []

    try:
        for path in Path(LAKEHOUSE_RAW_PATH).glob("*.zip"):
            arquivos_existentes.append({
                "arquivo": path.name,
                "data_modificacao": datetime.fromtimestamp(path.stat().st_mtime)
            })
    except Exception as e:
        print(f"Erro ao acessar a pasta do Lakehouse: {e}")

    # Garante que o DataFrame tenha as colunas certas, mesmo se estiver vazio
    return pd.DataFrame(arquivos_existentes, columns=["arquivo", "data_modificacao"])



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================
# Função 3: Fazer download incremental
# =============================================

# Setup de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def baixar_e_salvar_incremental(df_portal: pd.DataFrame, df_lakehouse: pd.DataFrame) -> List[str]:
    novos_arquivos = []

    # Pré-processamento: cria dicionário com últimas datas do Lakehouse
    d_lakehouse = df_lakehouse.set_index("arquivo")["data_modificacao"].to_dict()

    for row in df_portal.itertuples(index=False):
        nome = row.arquivo
        data_portal = row.data_modificacao
        url = row.url

        data_lakehouse = d_lakehouse.get(nome)

        if data_lakehouse is None or (data_portal and data_portal > data_lakehouse):
            try:
                logging.info(f"Baixando: {nome}")
                response = requests.get(url, headers=HEADERS, timeout=20)

                if response.status_code == 200:
                    destino = Path(f"{LAKEHOUSE_RAW_PATH}/{nome}")
                    destino.write_bytes(response.content)
                    novos_arquivos.append(nome)
                    time.sleep(random.uniform(5, 10))  # Anti-robô
                else:
                    logging.warning(f"Falha no download: {nome} - Status: {response.status_code}")

            except Exception as e:
                logging.error(f"Erro ao baixar {nome}: {e}")

    logging.info(f" {len(novos_arquivos)} arquivos baixados.")
    return novos_arquivos


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =========================================================
# Executar funções - criar depois função main
# ========================================================

# 1. Lista arquivos no portal CVM
df_portal = listar_arquivos_portal_cvm()
df_lakehouse = listar_arquivos_lakehouse()
novos = baixar_e_salvar_incremental(df_portal, df_lakehouse)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
