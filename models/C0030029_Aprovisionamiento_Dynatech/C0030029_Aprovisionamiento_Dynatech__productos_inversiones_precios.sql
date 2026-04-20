{{
  config({    
    "materialized": "incremental",
    "alias": "productos_inversiones_precios",
    "database": "dev_business",
    "incremental_strategy": "delete+insert",
    "schema": "productos",
    "unique_key": ["fecha_dato", "codigo_valor"]
  })
}}

WITH dbo_dynprecios AS (

  {#VisualGroup: HistoricodePrecios#}
  {#Overwrites the dataset for dynamic pricing in the curated database.#}
  SELECT * 
  
  FROM {{ source('dev_curated_uyasdbtest_dynvaldwsantander', 'dbo_dynprecios') }}

),

Filter_DynPreFrePrecio AS (

  {#VisualGroup: HistoricodePrecios#}
  {#Filters dynamic pricing data based on a specific date part.#}
  SELECT * 
  
  FROM dbo_dynprecios AS in0
  
  WHERE data_date_part == {{ var('ref_data_date_part') }}

),

AlteryxSelect_105 AS (

  {#VisualGroup: HistoricodePrecios#}
  {#Transforms and standardizes pricing data for analysis.#}
  SELECT 
    CAST(DynPreFec AS TIMESTAMP) AS fecha_dato,
    DynPreVlrId AS codigo_valor,
    DynPrePrecio AS precio_valor,
    DynPreMonSmb AS moneda_precio,
    CAST(DynPreFecPrecio AS TIMESTAMP) AS fecha_precio
  
  FROM Filter_DynPreFrePrecio AS in0

),

Formula_106_0 AS (

  {#VisualGroup: HistoricodePrecios#}
  SELECT 
    CAST((
      CASE
        WHEN CAST((moneda_precio = 'EURO') AS BOOLEAN)
          THEN 'EUR'
        ELSE moneda_precio
      END
    ) AS string) AS moneda_precio,
    * EXCEPT (`moneda_precio`)
  
  FROM AlteryxSelect_105 AS in0

),

AlteryxSelect_368 AS (

  {#VisualGroup: HistoricodePrecios#}
  {#Standardizes financial data for analysis by converting dates and currency formats.#}
  SELECT 
    CAST(fecha_dato AS DATE) AS fecha_dato,
    codigo_valor AS codigo_valor,
    CAST(fecha_precio AS DATE) AS fecha_precio,
    precio_valor AS precio_valor,
    CAST(moneda_precio AS string) AS moneda_precio
  
  FROM Formula_106_0 AS in0

),

cast_productos_inversiones_precios AS (

  {#VisualGroup: HistoricodePrecios#}
  {#Transforms and standardizes investment product prices for financial analysis.#}
  SELECT 
    {{ var('ref_data_date_part') }} AS ref_data_date_part,
    current_timestamp() AS ref_timestamp_procesamiento,
    fecha_dato AS fecha_dato,
    codigo_valor AS codigo_valor,
    fecha_precio AS fecha_precio,
    CAST(precio_valor AS DECIMAL (19, 5)) AS precio_valor,
    moneda_precio AS moneda_precio
  
  FROM AlteryxSelect_368 AS in0

)

{#VisualGroup: HistoricodePrecios#}
{#Merges investment product prices into the business dataset with an incremental update strategy.#}
SELECT *

FROM cast_productos_inversiones_precios
