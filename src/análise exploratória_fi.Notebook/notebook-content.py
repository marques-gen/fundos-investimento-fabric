# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "89f4ebd1-af7a-4952-b5fa-35e29f7ee358",
# META       "default_lakehouse_name": "lakehouse_silver",
# META       "default_lakehouse_workspace_id": "61df9dee-1bf7-4985-975b-82a6be49a59a",
# META       "known_lakehouses": [
# META         {
# META           "id": "89f4ebd1-af7a-4952-b5fa-35e29f7ee358"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "8f414a67-85b3-8506-4728-15fb7f59e460",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Realizar filtro considerando apenas a última data de modificação.

# CELL ********************

# ===================================================================================
# Considerar ultima alteração de acordo com a competência e fundo - Aplicar Tranformações na Silver
# ===================================================================================
spark.sql("""
    CREATE OR REPLACE TEMP VIEW vw_fi AS
    select
        t1.TP_FUNDO
        ,t1.CNPJ_FUNDO
        ,cast(t1.DT_COMPTC as date) as DT_COMPTC
        ,cast(t1.VL_TOTAL as decimal(18,2)) as VL_TOTAL
        ,cast(t1.VL_QUOTA as decimal(18,2)) as VL_QUOTA
        ,cast(t1.VL_PATRIM_LIQ as decimal(18,2)) as VL_PATRIM_LIQ
        ,cast(t1.CAPTC_DIA AS decimal(18,2)) as CAPTC_DIA
        ,cast(t1.RESG_DIA as DECIMAL(18,2)) as RESG_DIA
        ,data_modificacao as DATA_MODIFICACAO
        ,row_number() over( partition by t1.CNPJ_FUNDO,t1.TP_FUNDO,t1.DT_COMPTC order by t1.data_modificacao desc) as nr_ord

from inf_diario_fi_bronze_consolidacao t1
where T1.DT_COMPTC BETWEEN '2024-01-01' AND '2024-12-31' -- Amostra de dados

""")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Criar coluna para cálculo da rentabilidade diária dos fundos.
# 
# Rentabilidade Diária= (Cota Hoje−Cota Ontem) /cota Ontem
# 
# - visão acumulada por ano,semestre e mês
# - Variação acumulada por ano, semestre, mês e dia
# 


# CELL ********************

# ===============================================================
# Criar colunas para cálculo da rentabilidade diária dos fundos - Aplicar transformações na Silver
# ================================================================
df_rent_diaria=spark.sql("""
                            SELECT 
                                TP_FUNDO,
                                CNPJ_FUNDO,
                                DT_COMPTC,
                                VL_TOTAL,
                                VL_QUOTA,
                                VL_PATRIM_LIQ,
                                CAPTC_DIA,
                                RESG_DIA,
                                DATA_MODIFICACAO,
                                
                                -- Captura a VL_QUOTA do dia anterior para cada fundo
                                LAG(VL_QUOTA) OVER (
                                    PARTITION BY CNPJ_FUNDO
                                                ,TP_FUNDO 
                                    ORDER BY DT_COMPTC
                                ) AS VL_QUOTA_ANTERIOR
                                
                                -- Cálculo da rentabilidade diária
                                ,CASE 
                                    WHEN LAG(VL_QUOTA) OVER (
                                                            PARTITION BY
                                                                CNPJ_FUNDO
                                                                ,TP_FUNDO
                                                            ORDER BY DT_COMPTC) IS NOT NULL

                                    THEN (VL_QUOTA - LAG(VL_QUOTA) OVER (
                                                                        PARTITION BY
                                                                            CNPJ_FUNDO
                                                                            ,TP_FUNDO
                                                                        ORDER BY 
                                                                            DT_COMPTC)
                                                                        ) 

                                        / 
                                        LAG(VL_QUOTA) OVER (
                                                            PARTITION BY
                                                                CNPJ_FUNDO
                                                                ,TP_FUNDO
                                                            ORDER BY
                                                                DT_COMPTC
                                                                )
                                    ELSE NULL
                                END AS RENTAB_DIARIA

                            FROM vw_fi
                            WHERE NR_ORD=1

""")

df_rent_diaria.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 

# MARKDOWN ********************

# #### Quando finalizar as transformações aplicando regras de negócios/tratamento e carregar oa dados limpos/processados nas estruturas de armazenamento(lakehouses) conseguiremos responder as seguinte perguntas:
# 
# 
# - Fundos com maior retorno acumulado dinamicamente por período.
# 
# - Maior volatilidade(risco).
# 
# - Maior número de cotistas
# 
# - Fundos com maior captação líquida (CAPTC_DIA - RESG_DIA)
# 
# - Fundos com Maiores e menores Patrimônio líquido.

