{{
  config({    
    "materialized": "incremental",
    "alias": "productos_inversiones_contratos",
    "database": "dev_business",
    "incremental_strategy": "delete+insert",
    "schema": "productos",
    "unique_key": ["ref_data_date_part"]
  })
}}

WITH dbo_dynclientesapp AS (

  SELECT * 
  
  FROM {{ source('dev_curated_uyasdbtest_dynvaldwsantander', 'dbo_dynclientesapp') }}

),

fr_rf_dcp AS (

  {#Filters client data based on a specific reference date part.#}
  SELECT * 
  
  FROM dbo_dynclientesapp AS in0
  
  WHERE {{ var('ref_data_date_part') }} == data_date_part

),

AlteryxSelect_202 AS (

  SELECT *
  
  FROM fr_rf_dcp AS in0

),

Filter_72 AS (

  SELECT * 
  
  FROM AlteryxSelect_202 AS in0
  
  WHERE (
          NOT(
            DynCliAEstado = 'ANU')
        )

),

Formula_71_0 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        (
          REGEXP_REPLACE(
            (FORMAT_NUMBER(CAST(((EXTRACT(YEAR FROM DynCliAFec) * 100) + EXTRACT(MONTH FROM DynCliAFec)) AS DOUBLE), 0)), 
            ',', 
            '__THS__')
        ), 
        '__THS__', 
        '')
    ) AS string) AS AAAAMM,
    *
  
  FROM Filter_72 AS in0

),

AlteryxSelect_78 AS (

  SELECT 
    CAST(DynCliACtdId AS string) AS DynCliACtdId,
    * EXCEPT (`DynCliACtdId`)
  
  FROM Formula_71_0 AS in0

),

AlteryxSelect_65 AS (

  SELECT *
  
  FROM {{ ref('C0030029_Aprovisionamiento_Dynatech__AlteryxSelect_65')}}

),

Filter_74 AS (

  {#Selects records where the primary holder is identified.#}
  SELECT * 
  
  FROM AlteryxSelect_65 AS in0
  
  WHERE es_primer_titular = True

),

Join_76_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`es_primer_titular`, 
    `fecha_dato`, 
    `fecha_alta_titular`, 
    `estado_persona`, 
    `fecha_inicio_w8`, 
    `fecha_vencimiento_w8`, 
    `estado_w8`, 
    `fecha_inicio_3103`, 
    `fecha_vencimiento_3103`, 
    `estado_3103`, 
    `fecha_inicio_306`, 
    `fecha_vencimiento_306`, 
    `estado_306`, 
    `AAAAMM`, 
    `numero_contrato_inversiones`)
  
  FROM AlteryxSelect_78 AS in0
  INNER JOIN Filter_74 AS in1
     ON (in0.DynCliACtdId = in1.numero_contrato_inversiones)

),

AlteryxSelect_70 AS (

  SELECT 
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynCliAFec AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynCliAFec AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynCliAFec AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynCliAFec AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(DynCliAFec AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_dato,
    idf_pers_ods AS idf_pers_ods,
    DynCliACtdId AS numero_contrato_inversiones,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynCliAFecApe AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynCliAFecApe AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynCliAFecApe AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynCliAFecApe AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(DynCliAFecApe AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_alta_contrato_inversiones,
    DynCliAEstSis AS estado_cliente,
    DynCliAOfiId AS numero_oficial_inversiones,
    DynCliAOfiNom AS nombre_oficial_inversiones,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynCliAPerfRieFecMod AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynCliAPerfRieFecMod AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynCliAPerfRieFecMod AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynCliAPerfRieFecMod AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(DynCliAPerfRieFecMod AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_modificacion_perfil_riesgo,
    DynCliAPerfRieNom AS perfil_de_riesgo,
    DynCliARenI30D AS rentabilidad_30_dias_absoluto,
    DynCliARenP30D AS rentabilidad_30_dias_porcentual,
    DynCliARenI3M AS rentabilidad_90_dias_absoluto,
    DynCliARenP3M AS rentabilidad_90_dias_porcentual,
    DynCliARenI6M AS rentabilidad_180_dias_absoluta,
    DynCliARenP6M AS rentabilidad_180_dias_porcentual,
    DynCliARenIAAct AS rentabilidad_ytd_absoluta,
    DynCliARenPAAct AS rentabilidad_ytd_porcentual,
    DynCliARenIAAnt AS rentabilidad_ano_anterior_absoluta,
    DynCliARenPAAnt AS rentabilidad_ano_anterior_porcentual,
    DynCliARenI1A AS rentabiliad_anual_absoluta,
    DynCliARenP1A AS rentabilidad_anual_porcentual,
    DynCliARenI2A AS rentabilidad_2_anos_absoluta,
    DynCliARenP2A AS rentabilidad_2_anos_porcentual,
    AAAAMM AS AAAAMM
  
  FROM Join_76_inner AS in0

),

cast_productos_inversiones_contratos AS (

  {#Standardizes investment contract data for performance analysis over various timeframes.#}
  SELECT 
    {{ var('ref_data_date_part') }} AS ref_data_date_part,
    current_timestamp() AS ref_timestamp_procesamiento,
    CAST(fecha_dato AS DATE) AS fecha_dato,
    idf_pers_ods AS idf_pers_ods,
    numero_contrato_inversiones AS numero_contrato_inversiones,
    CAST(fecha_alta_contrato_inversiones AS DATE) AS fecha_alta_contrato_inversiones,
    estado_cliente AS estado_cliente,
    numero_oficial_inversiones AS numero_oficial_inversiones,
    nombre_oficial_inversiones AS nombre_oficial_inversiones,
    CAST(fecha_modificacion_perfil_riesgo AS DATE) AS fecha_modificacion_perfil_riesgo,
    perfil_de_riesgo AS perfil_de_riesgo,
    CAST(rentabilidad_30_dias_absoluto AS DECIMAL (19, 2)) AS rentabilidad_30_dias_absoluto,
    CAST(rentabilidad_30_dias_porcentual AS DECIMAL (13, 4)) AS rentabilidad_30_dias_porcentual,
    CAST(rentabilidad_90_dias_absoluto AS DECIMAL (19, 2)) AS rentabilidad_90_dias_absoluto,
    CAST(rentabilidad_90_dias_porcentual AS DECIMAL (19, 4)) AS rentabilidad_90_dias_porcentual,
    CAST(rentabilidad_180_dias_absoluta AS DECIMAL (19, 2)) AS rentabilidad_180_dias_absoluta,
    CAST(rentabilidad_180_dias_absoluta AS DECIMAL (19, 4)) AS rentabilidad_180_dias_porcentual,
    CAST(rentabilidad_ytd_absoluta AS DECIMAL (19, 2)) AS rentabilidad_ytd_absoluta,
    CAST(rentabilidad_ytd_porcentual AS DECIMAL (13, 4)) AS rentabilidad_ytd_porcentual,
    CAST(rentabilidad_ano_anterior_absoluta AS DECIMAL (19, 2)) AS rentabilidad_ano_anterior_absoluta,
    CAST(rentabilidad_ano_anterior_porcentual AS DECIMAL (19, 4)) AS rentabilidad_ano_anterior_porcentual,
    CAST(rentabiliad_anual_absoluta AS DECIMAL (19, 2)) AS rentabiliad_anual_absoluta,
    CAST(rentabilidad_anual_porcentual AS DECIMAL (13, 4)) AS rentabilidad_anual_porcentual,
    CAST(rentabilidad_2_anos_absoluta AS DECIMAL (19, 2)) AS rentabilidad_2_anos_absoluta,
    CAST(rentabilidad_2_anos_porcentual AS DECIMAL (19, 4)) AS rentabilidad_2_anos_porcentual,
    AAAAMM AS AAAAMM
  
  FROM AlteryxSelect_70 AS in0

)

SELECT *

FROM cast_productos_inversiones_contratos
