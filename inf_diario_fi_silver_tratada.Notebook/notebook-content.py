# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "17881140-eaa2-46ea-a531-f18138b080af",
# META       "default_lakehouse_name": "lakehouse_fundos_investimento",
# META       "default_lakehouse_workspace_id": "61df9dee-1bf7-4985-975b-82a6be49a59a",
# META       "known_lakehouses": [
# META         {
# META           "id": "17881140-eaa2-46ea-a531-f18138b080af"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# =======================================================================
# Considerar ultima alteração de acordo com a competência e cnpj_fundo
# ======================================================================
spark.sql("""
    CREATE OR REPLACE TEMP VIEW vw_bronze AS
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
        ,row_number() over( partition by t1.CNPJ_FUNDO,t1.DT_COMPTC order by t1.data_modificacao desc) as nr_ord

from inf_diario_fi_bronze_consolidacao t1
""")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =======================================================================
# criar camada silver
#=======================================================================

spark.sql("""
    CREATE TABLE IF NOT EXISTS inf_diario_fi_silver_tratada (
        CNPJ_FUNDO string
        ,DT_COMPTC date 
        ,VL_TOTAL decimal(18,2)
        ,VL_QUOTA decimal(18,2) 
        ,VL_PATRIM_LIQ decimal(18,2)
        ,CAPTC_DIA decimal(18,2)
        ,RESG_DIA decimal(18,2)
        ,DATA_MODIFICACAO timestamp
        
        
    )
    USING DELTA
    PARTITIONED BY (DT_COMPTC)
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


spark.sql("""
INSERT INTO inf_diario_fi_silver_tratada
SELECT
        t1.CNPJ_FUNDO
        ,t1.DT_COMPTC
        ,t1.VL_TOTAL
        ,t1.VL_QUOTA
        ,t1.VL_PATRIM_LIQ
        ,t1.CAPTC_DIA
        ,t1.RESG_DIA
        ,t1.DATA_MODIFICACAO

from vw_bronze t1
where not exists(
                    select 1

                    from inf_diario_fi_silver_tratada t2
                    where t1.CNPJ_FUNDO=T2.CNPJ_FUNDO
                    AND T1.DT_COMPTC=T2.DT_COMPTC
                    AND T1.DATA_MODIFICACAO=T2.DATA_MODIFICACAO
                    

                    )
AND T1.NR_ORD=1 -- Considera ultima alteração

"""
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT count(1) FROM lakehouse_fundos_investimento.inf_diario_fi_silver_tratada")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
