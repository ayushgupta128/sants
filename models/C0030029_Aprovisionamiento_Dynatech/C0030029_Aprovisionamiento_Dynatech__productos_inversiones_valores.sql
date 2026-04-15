{{
  config({    
    "materialized": "table",
    "alias": "productos_inversiones_valores",
    "database": "dev_business",
    "schema": "productos"
  })
}}

WITH dbo_dynvaloresapp AS (

  SELECT * 
  
  FROM {{ source('dev_curated_uyasdbtest_dynvaldwsantander', 'dbo_dynvaloresapp') }}

),

Filter_Data_Date_Part AS (

  {#Filters records based on a specific date part for targeted analysis.#}
  SELECT * 
  
  FROM dbo_dynvaloresapp AS in0
  
  WHERE data_date_part == {{ var('ref_data_date_part') }}

),

Summarize_101 AS (

  SELECT MAX(DynValAFec) AS Max_DynValAFec
  
  FROM Filter_Data_Date_Part AS in0

),

AppendFields_103 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Summarize_101 AS in0
  INNER JOIN Filter_Data_Date_Part AS in1
     ON (1 = 1)

),

AlteryxSelect_99 AS (

  SELECT 
    DynValAVlrId AS codigo_valor,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(Max_DynValAFec AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(Max_DynValAFec AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(Max_DynValAFec AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(Max_DynValAFec AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(Max_DynValAFec AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_dato,
    DynValAVlrNom AS nombre_valor,
    DynValAEstValor AS estado_valor,
    DynValAEspId AS codigo_especie_valor,
    DynValAEspNom AS descripcion_especie_valor,
    DynValATipoInsId AS codigo_tipo_valor,
    DynValATipoInsNom AS descripcion_tipo_valor,
    DynValAEmiNom AS nombre_emisor,
    DynValAEmiPai AS pais_emisor,
    DynValAVlrSector AS sector_emisor,
    DynValAISIN AS isin,
    DynValATicker AS ticker,
    DynValACodBlm AS codigo_bloomberg,
    DynValACUSIP AS cusip,
    DynValAEmiDes AS descripcion_emision,
    DynValACusNom AS nombre_custodio,
    DynValACusCalif AS calificacion_custodio,
    DynValAMonEmiSmb AS moneda_emision,
    DynValAMonAjuSmb AS moneda_ajuste,
    DynValAMonEfeSmb AS moneda_efectivo,
    DynValAMonCupSmb AS moneda_cupon,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynValAFecVto AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynValAFecVto AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynValAFecVto AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynValAFecVto AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(DynValAFecVto AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_vencimiento,
    DynValAUnidMinNeg AS unidad_minima_compra,
    DynValAIncremento AS unidad_minima_incremento,
    DynValATipoPrecio AS tipo_precio,
    DynValATipoCupon AS tipo_cupon,
    DynValAFrecCupon AS frecuencia_cupon,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynValAPrxCupon AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynValAPrxCupon AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynValAPrxCupon AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynValAPrxCupon AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(DynValAPrxCupon AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_proximo_cupon,
    DynValABase AS base_calculo_dias,
    DynValAPrxAmort AS fecha_proxima_amortizacion,
    DynValACalifSP AS calificacion_sp,
    DynValAFecDesdeSP AS fecha_calificacion_sp,
    DynValAFecEmision AS fecha_emision,
    DynValAMontoEmitido AS monto_emision,
    DynValATipoColateral AS tipo_colateral,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynValAFecAlt AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynValAFecAlt AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynValAFecAlt AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynValAFecAlt AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(DynValAFecAlt AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_alta,
    DynValAEstado AS estado_valor_en_sistema
  
  FROM AppendFields_103 AS in0

),

Formula_97_0 AS (

  SELECT 
    CAST(TRIM(codigo_valor) AS string) AS codigo_valor,
    CAST(TRIM(isin) AS string) AS isin,
    CAST((
      CASE
        WHEN CAST((moneda_emision = 'EURO') AS BOOLEAN)
          THEN 'EUR'
        ELSE moneda_emision
      END
    ) AS string) AS moneda_emision,
    CAST((
      CASE
        WHEN CAST((moneda_ajuste = 'EURO') AS BOOLEAN)
          THEN 'EUR'
        ELSE moneda_ajuste
      END
    ) AS string) AS moneda_ajuste,
    CAST((
      CASE
        WHEN CAST((moneda_efectivo = 'EURO') AS BOOLEAN)
          THEN 'EUR'
        ELSE moneda_efectivo
      END
    ) AS string) AS moneda_efectivo,
    CAST((
      CASE
        WHEN CAST((moneda_cupon = 'EURO') AS BOOLEAN)
          THEN 'EUR'
        ELSE moneda_cupon
      END
    ) AS string) AS moneda_cupon,
    * EXCEPT (`moneda_cupon`, `codigo_valor`, `isin`, `moneda_emision`, `moneda_efectivo`, `moneda_ajuste`)
  
  FROM AlteryxSelect_99 AS in0

),

AlteryxSelect_140 AS (

  {#Provides detailed financial instrument data including issuer, currency, and ratings for comprehensive analysis.#}
  SELECT 
    codigo_valor AS codigo_valor,
    CAST(fecha_dato AS DATE) AS fecha_dato,
    nombre_valor AS nombre_valor,
    estado_valor AS estado_valor,
    codigo_especie_valor AS codigo_especie_valor,
    descripcion_especie_valor AS descripcion_especie_valor,
    codigo_tipo_valor AS codigo_tipo_valor,
    descripcion_tipo_valor AS descripcion_tipo_valor,
    nombre_emisor AS nombre_emisor,
    pais_emisor AS pais_emisor,
    sector_emisor AS sector_emisor,
    isin AS isin,
    ticker AS ticker,
    codigo_bloomberg AS codigo_bloomberg,
    cusip AS cusip,
    descripcion_emision AS descripcion_emision,
    nombre_custodio AS nombre_custodio,
    calificacion_custodio AS calificacion_custodio,
    CAST(moneda_emision AS string) AS moneda_emision,
    CAST(moneda_ajuste AS string) AS moneda_ajuste,
    CAST(moneda_efectivo AS string) AS moneda_efectivo,
    CAST(moneda_cupon AS string) AS moneda_cupon,
    CAST(fecha_vencimiento AS date) AS fecha_vencimiento,
    CAST(unidad_minima_compra AS DECIMAL (19, 5)) AS unidad_minima_compra,
    CAST(unidad_minima_compra AS DECIMAL (19, 5)) AS unidad_minima_incremento,
    tipo_precio AS tipo_precio,
    tipo_cupon AS tipo_cupon,
    base_calculo_dias AS base_calculo_dias,
    frecuencia_cupon AS frecuencia_cupon,
    CAST(fecha_proximo_cupon AS DATE) AS fecha_proximo_cupon,
    fecha_proxima_amortizacion AS fecha_proxima_amortizacion,
    calificacion_sp AS calificacion_sp,
    fecha_calificacion_sp AS fecha_calificacion_sp,
    fecha_emision AS fecha_emision,
    CAST(monto_emision AS DECIMAL (19, 5)) AS monto_emision,
    tipo_colateral AS tipo_colateral,
    CAST(fecha_alta AS date) AS fecha_alta,
    estado_valor_en_sistema AS estado_valor_en_sistema
  
  FROM Formula_97_0 AS in0

),

cast_productos_inversiones_valores AS (

  {#Transforms and standardizes investment product data for comprehensive reporting.#}
  SELECT 
    {{ var('ref_data_date_part') }} AS ref_data_date_part,
    current_timestamp() AS ref_timestamp_procesamiento,
    codigo_valor AS codigo_valor,
    CAST(fecha_dato AS DATE) AS fecha_dato,
    nombre_valor AS nombre_valor,
    estado_valor AS estado_valor,
    codigo_especie_valor AS codigo_especie_valor,
    descripcion_especie_valor AS descripcion_especie_valor,
    codigo_tipo_valor AS codigo_tipo_valor,
    descripcion_tipo_valor AS descripcion_tipo_valor,
    nombre_emisor AS nombre_emisor,
    pais_emisor AS pais_emisor,
    sector_emisor AS sector_emisor,
    isin AS isin,
    ticker AS ticker,
    codigo_bloomberg AS codigo_bloomberg,
    cusip AS cusip,
    descripcion_emision AS descripcion_emision,
    nombre_custodio AS nombre_custodio,
    calificacion_custodio AS calificacion_custodio,
    moneda_emision AS moneda_emision,
    moneda_ajuste AS moneda_ajuste,
    moneda_efectivo AS moneda_efectivo,
    moneda_cupon AS moneda_cupon,
    CAST(fecha_vencimiento AS DATE) AS fecha_vencimiento,
    CAST(unidad_minima_compra AS DECIMAL (19, 5)) AS unidad_minima_compra,
    CAST(unidad_minima_incremento AS DECIMAL (19, 5)) AS unidad_minima_incremento,
    tipo_precio AS tipo_precio,
    tipo_cupon AS tipo_cupon,
    base_calculo_dias AS base_calculo_dias,
    frecuencia_cupon AS frecuencia_cupon,
    CAST(fecha_proximo_cupon AS DATE) AS fecha_proximo_cupon,
    CAST(fecha_proxima_amortizacion AS date) AS fecha_proxima_amortizacion,
    calificacion_sp AS calificacion_sp,
    CAST(fecha_calificacion_sp AS date) AS fecha_calificacion_sp,
    CAST(fecha_emision AS date) AS fecha_emision,
    CAST(monto_emision AS DECIMAL (19, 5)) AS monto_emision,
    tipo_colateral AS tipo_colateral,
    fecha_alta AS fecha_alta,
    estado_valor_en_sistema AS estado_valor_en_sistema
  
  FROM AlteryxSelect_140 AS in0

)

SELECT *

FROM cast_productos_inversiones_valores
