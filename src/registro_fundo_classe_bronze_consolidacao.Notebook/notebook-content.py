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
# Instalação bibliotecas, depois criar ambiente virtual
# ==============================================================

%pip install --upgrade pip
%pip install pandera


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ========================================================================
# Imports e configuração do logger
# ========================================================================
import os
import zipfile
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import year, month
from delta.tables import DeltaTable

# Diretórios do Lakehouse
caminho_raw = "/lakehouse/default/Files/landing/inf_diario_fi"
caminho_bronze= "/lakehouse/default/Files/bronze"
tabela_bronze = "inf_diario_fi_bronze_consolidacao"

# ⚙️ Configuração de Logging
log_path = "/lakehouse/default/Files/logs/log_validacao.txt"

logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(message)s')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inicializar SparkSession
spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Schema Pandera para validação dos dados
import pandera as pa
from pandera import Column, DataFrameSchema
schema = pa.DataFrameSchema(
    {
    "TP_FUNDO": pa.Column(str),
    "CNPJ_FUNDO": pa.Column(str),
    "DT_COMPTC": pa.Column(str,coerce=True),  # formato validar depois
    "VL_TOTAL": pa.Column(str,nullable=True),
    "VL_QUOTA": pa.Column(str),
    "VL_PATRIM_LIQ": pa.Column(str),
    "CAPTC_DIA": pa.Column(str),
    "RESG_DIA": pa.Column(str),
    "NR_COTST": pa.Column(int),
},
strict=False  # Permiter colunas não definidas no schema

)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==================================================================
# Tratativa erros
# ==================================================================

de_para_colunas = {
    "TP_FUNDO_CLASSE": "TP_FUNDO",
    "CNPJ_FUNDO_CLASSE": "CNPJ_FUNDO"
}


def padronizar_colunas(df: pd.DataFrame, de_para: dict) -> pd.DataFrame:
    """
    Renomeia colunas com base em um dicionário de mapeamento "DE → PARA".
    Remove espaços em branco antes do renomeio.
    
    Parâmetros:
    - df: DataFrame a ser tratado.
    - de_para: dicionário com nomes incorretos como chave e corretos como valor.

    Retorna:
    - DataFrame com colunas renomeadas.
    """
    # Limpa espaços à esquerda/direita dos nomes de colunas
    df.columns = df.columns.str.strip()

    # Detecta colunas que serão renomeadas
    colunas_renomeadas = {col: novo for col, novo in de_para.items() if col in df.columns}
    if colunas_renomeadas:
        logging.info(f"Renomeando colunas: {colunas_renomeadas}")
        df = df.rename(columns=colunas_renomeadas)
    else:
        logging.info("Nenhuma coluna para renomear encontrada.")

    return df

# ==================================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leitura e validação dos arquivos zip

arquivos_validos = []
dfs_validos = []
erros_schema = []  # <-- Novo: Lista para registrar os erros

for arquivo in os.listdir(caminho_raw):
    if arquivo.endswith(".zip"):
        caminho_arquivo = os.path.join(caminho_raw, arquivo)
        with zipfile.ZipFile(caminho_arquivo, 'r') as zip_ref:
            for nome_arquivo in zip_ref.namelist():
                with zip_ref.open(nome_arquivo) as file:
                    try:
                        df = pd.read_csv(file, sep=";", encoding="latin1", decimal=",",dtype={'VL_QUOTA': str})                                          
                                        
                        df = padronizar_colunas(df, de_para_colunas) # verificar melhor abordagem
                        schema.validate(df, lazy=True)

                        df["arquivo"] = nome_arquivo
                        df["data_modificacao"] = datetime.fromtimestamp(os.path.getmtime(caminho_arquivo))
                        df["ano"] = pd.to_datetime(df["DT_COMPTC"], errors="coerce").dt.year
                        df["mes"] = pd.to_datetime(df["DT_COMPTC"], errors="coerce").dt.month

                        dfs_validos.append(df)
                        arquivos_validos.append(nome_arquivo)

                        logging.info(f"Arquivo válido: {nome_arquivo}")

                    except SchemaErrors as e:
                        erros = e.failure_cases.copy()
                        erros["arquivo"] = nome_arquivo  # Adiciona o nome do arquivo com erro
                        erros_schema.append(erros)

                        logging.warning(f"Schema inválido - {nome_arquivo}: {len(erros)} erros")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS inf_diario_fi_bronze_consolidacao (
    TP_FUNDO STRING,
    CNPJ_FUNDO STRING,
    DT_COMPTC STRING,
    VL_TOTAL STRING,
    VL_QUOTA STRING,
    VL_PATRIM_LIQ STRING,
    CAPTC_DIA STRING,
    RESG_DIA STRING,
    NR_COTST BIGINT,
    arquivo STRING,
    data_modificacao TIMESTAMP,
    ano INT,
    mes INT,
    ID_SUBCLASSE STRING
)
USING DELTA
""")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# ========================================================
#
# ========================================================
df_bronze = spark.sql("""
    SELECT DISTINCT arquivo, data_modificacao
    FROM inf_diario_fi_bronze_consolidacao
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# concatena todos os dataframes
df_final = pd.concat(dfs_validos, ignore_index=True)

# Conversão para Spark
df_spark = spark.createDataFrame(df_final)


# Comparar se existe novos arquivos
df_novos = df_spark.alias("novo").join(
    df_bronze.alias("bronze"),
    on=[
        df_spark["arquivo"] == df_bronze["arquivo"],
        df_spark["data_modificacao"] == df_bronze["data_modificacao"]
    ],
    how="anti"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================================
# # Persiste os dados na camada bronze e cria uma tabela gerenciada
# =================================================================================

from delta.tables import DeltaTable

#path_bronze = "Files/bronze/minha_tabela"
caminho_completo=os.path.join(caminho_bronze,tabela_bronze)

#df_spark.createOrReplaceTempView()

# Criação do banco Bronze (se não existir)
#spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# Escrita como tabela gerenciada Delta
if df_novos.count() > 0:
    df_spark.write.format("delta").mode("append").saveAsTable(tabela_bronze)
else:
     print("Nenhum dado novo para inserir.")

# Escrita na tabela delta não gerenciada
#df_spark.write.format("delta").mode("overwrite").saveAsTable("if_diario_fi_bronze_consolidacao",path='Files/bronze/if_diario_consolidacao')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(1)from inf_diario_fi_bronze_consolidacao

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
