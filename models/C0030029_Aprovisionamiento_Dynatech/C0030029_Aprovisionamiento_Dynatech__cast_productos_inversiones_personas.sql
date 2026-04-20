{{
  config({    
    "materialized": "ephemeral",
    "database": "akash_demos",
    "schema": "demos"
  })
}}

WITH AlteryxSelect_65 AS (

  SELECT *
  
  FROM {{ ref('C0030029_Aprovisionamiento_Dynatech__AlteryxSelect_65')}}

),

cast_productos_inversiones_personas AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  {#Generates a detailed report of investment contract holders, including personal status and key contract dates, to support compliance and customer management.#}
  SELECT 
    {{ var('ref_data_date_part') }} AS ref_data_date_part,
    current_timestamp() AS ref_timestamp_procesamiento,
    numero_contrato_inversiones AS numero_contrato_inversiones,
    idf_pers_ods AS idf_pers_ods,
    es_primer_titular AS es_primer_titular,
    CAST(fecha_dato AS DATE) AS fecha_dato,
    CAST(fecha_alta_titular AS DATE) AS fecha_alta_titular,
    estado_persona AS estado_persona,
    CAST(fecha_inicio_w8 AS DATE) AS fecha_inicio_w8,
    CAST(fecha_vencimiento_w8 AS DATE) AS fecha_vencimiento_w8,
    estado_w8 AS estado_w8,
    CAST(fecha_inicio_3103 AS DATE) AS fecha_inicio_3103,
    CAST(fecha_vencimiento_3103 AS DATE) AS fecha_vencimiento_3103,
    estado_3103 AS estado_3103,
    CAST(fecha_inicio_306 AS DATE) AS fecha_inicio_306,
    CAST(fecha_vencimiento_306 AS DATE) AS fecha_vencimiento_306,
    estado_306 AS estado_306,
    AAAAMM AS AAAAMM
  
  FROM AlteryxSelect_65 AS in0

)

SELECT *

FROM cast_productos_inversiones_personas
