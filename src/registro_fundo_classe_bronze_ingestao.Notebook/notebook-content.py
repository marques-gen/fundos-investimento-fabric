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

# ==============================================================
# Instalar bibliotecas, posteriormente criar ambiente virtual
# ==============================================================

#%pip install --upgrade pip
#%pip install pandera
# =====================================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==========================================================================
# # criar tabela para registro dos metadados dos arquivos ingeridos
# ==========================================================================

spark.sql("""
    CREATE TABLE IF NOT EXISTS ingestion_control_bronze (
      ---  id int GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)
        nome_arquivo string 
        ,data_modificacao timestamp
        ,origem_zip string
        ,tamanho_bytes long
        ,arquivo_ingestao string
        ,fonte string
        ,situacao_arquivo string
    --    ,constraint pk_arquivo_modificacao primary key(nome_arquivo,data_modificacao)
                    
        
    )
  --  USING DELTA
    --PARTITIONED BY (nome_arquivo,arquivo_ingestao),
   -- TBLPROPERTIES (
   -- 'delta.feature.identityColumns' = 'supported',
  --  'delta.constraints.enabled' = 'true'
  --              )
""")

# ===============================================================================


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===============================================================
# Impotação bibliotecas necessárias
# ===============================================================

from pathlib import Path
from datetime import datetime
import pandas as pd
import requests
import zipfile
import io
import os
from bs4 import BeautifulSoup
#from typing import Tuple
import shutil



# origem - Portal
url="https://dados.cvm.gov.br/dados/FI/CAD/DADOS/"

# Destino
destino="/lakehouse/default/Files/landing/registro_fundo_classe/"
# ===================================================================

#extenções a considerar
extensoes=['.zip','.csv']

# Desconsiderar ingestão dos arquivos abaixo.
arquivos_desnecessarios=['arquivo01']

# ========================================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# import os
# import requests
# from bs4 import BeautifulSoup
# from datetime import datetime
# 
# def listar_arquivos_portal_dict(url: str, destino: str) -> dict:
#     """
#     Lista arquivos (.zip, .csv) encontrados na página e retorna um dicionário:
#     {nome_arquivo: {"destino": destino, "data_modificacao": datetime}}
#     """
#     #extensoes = {'.zip', '.csv'}
#     resp = requests.get(url)
#     resp.raise_for_status()
#     soup = BeautifulSoup(resp.text, "html.parser")
# 
#     arquivos_dict = {}
#     for link in soup.find_all("a"):
#         href = link.get("href")
#         if href and os.path.splitext(href)[1].lower() in extensoes:
#             arquivo_url = os.path.join(url, href)
#             try:
#                 # Faz HEAD request para obter metadados sem baixar o arquivo
#                 head = requests.head(arquivo_url, timeout=5)
#                 last_mod = head.headers.get("Last-Modified")
#                 if last_mod:
#                     data_modificacao = datetime.strptime(last_mod, "%a, %d %b %Y %H:%M:%S %Z")
#                 else:
#                     None #data_modificacao = datetime.utcnow()
#             except:
#                 data_modificacao = datetime.utcnow()
# 
#             arquivos_dict[href] = {
#                 "destino": destino,
#                 "data_modificacao": data_modificacao
#             }
# 
#     return arquivos_dict


# CELL ********************

teste=listar_arquivos_portal_dict(url,destino)
teste

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def listar_arquivos_portal_dict(url: str, destino: str) -> dict:
    """
    Lista arquivos com extensões (.zip, .csv) encontrados na página e 
    retorna um dicionário {nome_sem_extensao: caminho_base}.
    """
    #extensoes = {'.zip', '.csv'}  # set → busca mais rápida que lista

    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    arquivos_dict = {}

    for link in soup.find_all("a"):
        href = link.get("href")
        if href and os.path.splitext(href)[1].lower() in extensoes:
            arquivos_dict[href] = destino  # mantém nome com extensão
            #arquivos_dict[os.path.splitext(href)[0]] = destino

    return arquivos_dict


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

teste=listar_arquivos_portal_dict(url,destino)
teste

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# def listar_arquivos_portal_dict(url: str, destino: str) -> dict:
#     """
#     Lista arquivos .zip encontrados na página e retorna um dicionário:
#     {nome_sem_extensao: caminho_base}
#     """
#     response = requests.get(url)
#     response.raise_for_status()
# 
#     soup = BeautifulSoup(response.text, "html.parser")
#     arquivos_dict = {}
# 
#     for link in soup.find_all("a"):
#         href = link.get("href")
#         if href and href.endswith(".zip"): # .zip
#             nome_sem_ext = os.path.splitext(href)[0]
#             arquivos_dict[nome_sem_ext] = destino
# 
#     return arquivos_dict


# CELL ********************

# ========================================================================
# Extrair metadados dos arquivos zipados
# ========================================================================
from email.utils import parsedate_to_datetime
def extrair_metadados_arquivos(url:str,destino:str, arquivos_desnecessarios: list[str] = []) -> pd.DataFrame:

    inputvalues=listar_arquivos_portal_dict(url,destino)#converter_df_dict(url,destino)

    for chave in arquivos_desnecessarios:
        inputvalues.pop(chave,None) # remove se existir, ignora se não

    # lista para armazenar os metadados dos arquivos.
    metadados = []

    for index, (arquivo_remoto,dest) in enumerate(inputvalues.items(), start=1):
        
        fileurl = f"{url}{arquivo_remoto}" # melhorar depois .zip
        print(f"Processando: {fileurl}")
        
        # Faz o download
        r = requests.get(fileurl)

        if arquivo_remoto.lower().endswith(".zip"):
            try:
                with zipfile.ZipFile(io.BytesIO(r.content)) as z:

                    for info in z.infolist():
                        # Metadados de cada arquivo no ZIP
                        nome_arquivo = info.filename
                        data_modificacao = datetime(*info.date_time)
                        tamanho = info.file_size

                        metadados.append({
                            "origem_zip": arquivo_remoto,
                            "nome_arquivo": nome_arquivo,
                            "data_modificacao": data_modificacao,
                            "tamanho_bytes": tamanho,
                            "arquivo_ingestao": Path(nome_arquivo).stem + "_" +
                                                data_modificacao.strftime("%Y%m%d%H%M%S") + 
                                                Path(nome_arquivo).suffix
                        })
            except zipfile.BadZipFile:
                        print(f"[AVISO] Arquivo ZIP inválido: {arquivo_remoto}")
        else:
            # Arquivo não zipado
            tamanho = len(r.content)
            if 'Last-Modified' in r.headers:
                data_modificacao = parsedate_to_datetime(r.headers['Last-Modified'])

            metadados.append({
                "origem_zip": None,
                "nome_arquivo": arquivo_remoto,
                "data_modificacao": data_modificacao,
                "tamanho_bytes": tamanho,
                "arquivo_ingestao": Path(arquivo_remoto).stem + "_" +
                                    data_modificacao.strftime("%Y%m%d%H%M%S") +
                                    Path(arquivo_remoto).suffix
            })

    df_metadados = pd.DataFrame(metadados)
    return df_metadados
    print(df_metadados)

# ==============================================================  


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

teste3=extrair_metadados_arquivos(url,destino)
teste3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===================================================================================
#  Verifica se existem novos arquivos a serem persistidos
# ===================================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Inicializa Spark
spark = SparkSession.builder.getOrCreate()


df_arquivos=extrair_metadados_arquivos(url,destino)

#  Converter Pandas para Spark
df_arquivos_spark=spark.createDataFrame(df_arquivos)

df_arquivos_spark = df_arquivos_spark.withColumn("data_modificacao", to_timestamp("data_modificacao"))

# Carrega a tabela Delta
df_delta = spark.read.format("delta").table("ingestion_control_bronze")

# Arquivos ainda não carregados
df_nao_carregados = df_arquivos_spark.join(
                                            df_delta,
                                            on=["nome_arquivo", "data_modificacao"],
                                            how="leftanti"  # Retorna apenas os arquivos ainda não existentes "leftsemi"  # Retorna apenas os arquivos já existentes
                                        )
                                      
arquivos_nao_carregados = [linha.nome_arquivo for linha in df_nao_carregados.select('nome_arquivo').collect()]
df_nao_carregados.show()
print(arquivos_nao_carregados)
# criar tabela controle incremental



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =======================================================================
# Scritp main - realiza ingestão incremental
#========================================================================
inputvalues=listar_arquivos_portal_dict(url,destino)

#for index, (dest,zipname) in enumerate(inputvalues,start=1):
for index, (zipname,dest) in enumerate(inputvalues.items(), start=1):
    pasta_saida=f"{dest}"
    fileurl = f"{url}{zipname}.zip" # melhorar depois
    print(pasta_saida)
    print(fileurl)
    r=requests.get(fileurl)
    z=zipfile.ZipFile(io.BytesIO(r.content))
    for text_file in z.infolist():

        nome_arquivo = text_file.filename
        data_modificacao = datetime(*text_file.date_time)
        tamanho = text_file.file_size
        arquivo_ingestao = Path(nome_arquivo).stem+"_"+data_modificacao.strftime("%Y%m%d%H%M%S")+Path(nome_arquivo).suffix# iso 8601
        caminho_destino=f"{pasta_saida}{arquivo_ingestao}"
        
        if nome_arquivo in arquivos_nao_carregados:
            z.extract(text_file,pasta_saida)

            # Renomear o arquivo com a data de modificação
            shutil.move(f"{pasta_saida}{nome_arquivo}", caminho_destino)
    df_nao_carregados.write.format("delta").mode("append").saveAsTable("ingestion_control_bronze")
       

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql   
# MAGIC SELECT 
# MAGIC     nome_arquivo
# MAGIC     ,count(1) as qtd_versoes
# MAGIC from ingestion_control_bronze limite
# MAGIC group BY
# MAGIC     nome_arquivo

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

