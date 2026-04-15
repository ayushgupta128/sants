{{
  config({    
    "materialized": "incremental",
    "alias": "productos_inversiones_transacciones",
    "database": "dev_business",
    "incremental_strategy": "delete+insert",
    "schema": "productos",
    "unique_key": ["id_movimiento"]
  })
}}

WITH AlteryxSelect_65 AS (

  SELECT *
  
  FROM {{ ref('C0030029_Aprovisionamiento_Dynatech__AlteryxSelect_65')}}

),

Filter_327 AS (

  {#Selects records where the primary holder is true.#}
  SELECT * 
  
  FROM AlteryxSelect_65 AS in0
  
  WHERE es_primer_titular = true

),

Join_349_inner AS (

  SELECT *
  
  FROM {{ ref('C0030029_Aprovisionamiento_Dynatech__Join_349_inner')}}

),

Formula_353_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((DynMoTMEfe = 'S') AS BOOLEAN)
          THEN 1
        ELSE 0
      END
    ) AS BOOLEAN) AS mueve_efectivo,
    CAST((
      CASE
        WHEN CAST((DynMoTMVN = 'S') AS BOOLEAN)
          THEN 1
        ELSE 0
      END
    ) AS BOOLEAN) AS mueve_nominales,
    CAST((
      REGEXP_REPLACE(
        (
          REGEXP_REPLACE(
            (FORMAT_NUMBER(CAST(((EXTRACT(YEAR FROM DynMoTFecVal) * 100) + EXTRACT(MONTH FROM DynMoTFecVal)) AS DOUBLE), 0)), 
            ',', 
            '__THS__')
        ), 
        '__THS__', 
        '')
    ) AS string) AS AAAAMM,
    *
  
  FROM Join_349_inner AS in0

),

Filter_352 AS (

  SELECT * 
  
  FROM Formula_353_0 AS in0
  
  WHERE (NOT CAST((CAST(DynMoTTipoReg AS string) IN ('C', 'REVV', 'V', 'REVC')) AS BOOLEAN))

),

Filter_352_reject AS (

  SELECT * 
  
  FROM Formula_353_0 AS in0
  
  EXCEPT ALL
  
  SELECT * 
  
  FROM Filter_352 AS in1

),

externos_bcu_tipo_de_cambio AS (

  SELECT * 
  
  FROM {{ source('dev_business.externos', 'externos_bcu_tipo_de_cambio') }}

),

f_data_date_part_cambio AS (

  {#Filters exchange rate data based on a specific date part.#}
  SELECT * 
  
  FROM externos_bcu_tipo_de_cambio AS in0
  
  WHERE {{ var('ref_data_date_part') }} == ref_data_date_part

),

AlteryxSelect_365 AS (

  SELECT 
    fecha_dato AS fecha_cotizacion,
    * EXCEPT (`fecha_dato`)
  
  FROM f_data_date_part_cambio AS in0

),

Filter_355 AS (

  SELECT * 
  
  FROM AlteryxSelect_365 AS in0
  
  WHERE (codigo_moneda = 'USD')

),

Join_356_inner AS (

  {#Integrates local exchange rate data with financial records for comprehensive currency analysis.#}
  SELECT 
    in1.tipo_de_cambio_local AS tipo_de_cambio_local_usd,
    in0.*
  
  FROM AlteryxSelect_365 AS in0
  INNER JOIN Filter_355 AS in1
     ON (in0.fecha_cotizacion = in1.fecha_cotizacion)

),

Formula_357_0 AS (

  SELECT 
    CAST((tipo_de_cambio_local / tipo_de_cambio_local_usd) AS DOUBLE) AS cotizacion_usd,
    *
  
  FROM Join_356_inner AS in0

),

Join_354_left AS (

  SELECT in0.*
  
  FROM Filter_352_reject AS in0
  ANTI JOIN Formula_357_0 AS in1
     ON ((in0.DynMoTFecVal = in1.fecha_cotizacion) AND (in0.moneda_movimiento = in1.codigo_moneda))

),

Summarize_361 AS (

  SELECT 
    AVG(cotizacion_usd) AS cotizacion_usd,
    AAAAMM AS AAAAMM,
    codigo_moneda AS codigo_moneda
  
  FROM Formula_357_0 AS in0
  
  GROUP BY 
    AAAAMM, codigo_moneda

),

Join_362_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`AAAAMM`, `codigo_moneda`)
  
  FROM Join_354_left AS in0
  INNER JOIN Summarize_361 AS in1
     ON ((in0.moneda_movimiento = in1.codigo_moneda) AND (in0.AAAAMM = in1.AAAAMM))

),

Union_363_reformat_2 AS (

  SELECT 
    AAAAMM AS AAAAMM,
    DynMoTBroNom AS DynMoTBroNom,
    DynMoTCatValor AS DynMoTCatValor,
    DynMoTCtaDep AS DynMoTCtaDep,
    DynMoTCtdId AS DynMoTCtdId,
    DynMoTCtraVlr AS DynMoTCtraVlr,
    DynMoTDesc AS DynMoTDesc,
    DynMoTDesc2 AS DynMoTDesc2,
    DynMoTEstSis AS DynMoTEstSis,
    DynMoTFecOpe AS DynMoTFecOpe,
    DynMoTFecVal AS DynMoTFecVal,
    DynMoTFormu AS DynMoTFormu,
    DynMoTFunId AS DynMoTFunId,
    DynMoTImpoMEfe AS DynMoTImpoMEfe,
    DynMoTMEfe AS DynMoTMEfe,
    DynMoTMVN AS DynMoTMVN,
    DynMoTMonEfe AS DynMoTMonEfe,
    DynMoTMonVlr AS DynMoTMonVlr,
    DynMoTOpiId AS DynMoTOpiId,
    DynMoTPrecio AS DynMoTPrecio,
    DynMoTPrtNom AS DynMoTPrtNom,
    DynMoTTipoPrec AS DynMoTTipoPrec,
    DynMoTTipoReg AS DynMoTTipoReg,
    DynMoTUsrAltVlr AS DynMoTUsrAltVlr,
    DynMoTVN AS DynMoTVN,
    DynMoTVlrEspNom AS DynMoTVlrEspNom,
    DynMoTVlrId AS DynMoTVlrId,
    DynMoTVlrNom AS DynMoTVlrNom,
    DynMotReqVal AS DynMotReqVal,
    CAST(cotizacion_usd AS DOUBLE) AS cotizacion_usd,
    moneda_movimiento AS moneda_movimiento,
    mueve_efectivo AS mueve_efectivo,
    mueve_nominales AS mueve_nominales
  
  FROM Join_362_inner AS in0

),

Join_354_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`fecha_cotizacion`, `codigo_moneda`, `tipo_de_cambio_local`, `AAAAMM`, `tipo_de_cambio_local_usd`)
  
  FROM Filter_352_reject AS in0
  INNER JOIN Formula_357_0 AS in1
     ON ((in0.DynMoTFecVal = in1.fecha_cotizacion) AND (in0.moneda_movimiento = in1.codigo_moneda))

),

Union_363_reformat_0 AS (

  SELECT 
    AAAAMM AS AAAAMM,
    DynMoTBroNom AS DynMoTBroNom,
    DynMoTCatValor AS DynMoTCatValor,
    DynMoTCtaDep AS DynMoTCtaDep,
    DynMoTCtdId AS DynMoTCtdId,
    DynMoTCtraVlr AS DynMoTCtraVlr,
    DynMoTDesc AS DynMoTDesc,
    DynMoTDesc2 AS DynMoTDesc2,
    DynMoTEstSis AS DynMoTEstSis,
    DynMoTFecOpe AS DynMoTFecOpe,
    DynMoTFecVal AS DynMoTFecVal,
    DynMoTFormu AS DynMoTFormu,
    DynMoTFunId AS DynMoTFunId,
    DynMoTImpoMEfe AS DynMoTImpoMEfe,
    DynMoTMEfe AS DynMoTMEfe,
    DynMoTMVN AS DynMoTMVN,
    DynMoTMonEfe AS DynMoTMonEfe,
    DynMoTMonVlr AS DynMoTMonVlr,
    DynMoTOpiId AS DynMoTOpiId,
    DynMoTPrecio AS DynMoTPrecio,
    DynMoTPrtNom AS DynMoTPrtNom,
    DynMoTTipoPrec AS DynMoTTipoPrec,
    DynMoTTipoReg AS DynMoTTipoReg,
    DynMoTUsrAltVlr AS DynMoTUsrAltVlr,
    DynMoTVN AS DynMoTVN,
    DynMoTVlrEspNom AS DynMoTVlrEspNom,
    DynMoTVlrId AS DynMoTVlrId,
    DynMoTVlrNom AS DynMoTVlrNom,
    DynMotReqVal AS DynMotReqVal,
    CAST(cotizacion_usd AS DOUBLE) AS cotizacion_usd,
    moneda_movimiento AS moneda_movimiento,
    mueve_efectivo AS mueve_efectivo,
    mueve_nominales AS mueve_nominales
  
  FROM Join_354_inner AS in0

),

Join_362_left AS (

  SELECT in0.*
  
  FROM Join_354_left AS in0
  ANTI JOIN Summarize_361 AS in1
     ON ((in0.moneda_movimiento = in1.codigo_moneda) AND (in0.AAAAMM = in1.AAAAMM))

),

Union_363_reformat_1 AS (

  SELECT 
    AAAAMM AS AAAAMM,
    DynMoTBroNom AS DynMoTBroNom,
    DynMoTCatValor AS DynMoTCatValor,
    DynMoTCtaDep AS DynMoTCtaDep,
    DynMoTCtdId AS DynMoTCtdId,
    DynMoTCtraVlr AS DynMoTCtraVlr,
    DynMoTDesc AS DynMoTDesc,
    DynMoTDesc2 AS DynMoTDesc2,
    DynMoTEstSis AS DynMoTEstSis,
    DynMoTFecOpe AS DynMoTFecOpe,
    DynMoTFecVal AS DynMoTFecVal,
    DynMoTFormu AS DynMoTFormu,
    DynMoTFunId AS DynMoTFunId,
    DynMoTImpoMEfe AS DynMoTImpoMEfe,
    DynMoTMEfe AS DynMoTMEfe,
    DynMoTMVN AS DynMoTMVN,
    DynMoTMonEfe AS DynMoTMonEfe,
    DynMoTMonVlr AS DynMoTMonVlr,
    DynMoTOpiId AS DynMoTOpiId,
    DynMoTPrecio AS DynMoTPrecio,
    DynMoTPrtNom AS DynMoTPrtNom,
    DynMoTTipoPrec AS DynMoTTipoPrec,
    DynMoTTipoReg AS DynMoTTipoReg,
    DynMoTUsrAltVlr AS DynMoTUsrAltVlr,
    DynMoTVN AS DynMoTVN,
    DynMoTVlrEspNom AS DynMoTVlrEspNom,
    DynMoTVlrId AS DynMoTVlrId,
    DynMoTVlrNom AS DynMoTVlrNom,
    DynMotReqVal AS DynMotReqVal,
    moneda_movimiento AS moneda_movimiento,
    mueve_efectivo AS mueve_efectivo,
    mueve_nominales AS mueve_nominales
  
  FROM Join_362_left AS in0

),

Union_363 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_363_reformat_0', 'Union_363_reformat_1', 'Union_363_reformat_2'], 
      [
        '[{"name": "AAAAMM", "dataType": "String"}, {"name": "DynMoTBroNom", "dataType": "String"}, {"name": "DynMoTCatValor", "dataType": "Decimal"}, {"name": "DynMoTCtaDep", "dataType": "String"}, {"name": "DynMoTCtdId", "dataType": "Integer"}, {"name": "DynMoTCtraVlr", "dataType": "Decimal"}, {"name": "DynMoTDesc", "dataType": "String"}, {"name": "DynMoTDesc2", "dataType": "String"}, {"name": "DynMoTEstSis", "dataType": "String"}, {"name": "DynMoTFecOpe", "dataType": "Timestamp"}, {"name": "DynMoTFecVal", "dataType": "Timestamp"}, {"name": "DynMoTFormu", "dataType": "String"}, {"name": "DynMoTFunId", "dataType": "Integer"}, {"name": "DynMoTImpoMEfe", "dataType": "Decimal"}, {"name": "DynMoTMEfe", "dataType": "String"}, {"name": "DynMoTMVN", "dataType": "String"}, {"name": "DynMoTMonEfe", "dataType": "Integer"}, {"name": "DynMoTMonVlr", "dataType": "Integer"}, {"name": "DynMoTOpiId", "dataType": "Decimal"}, {"name": "DynMoTPrecio", "dataType": "Decimal"}, {"name": "DynMoTPrtNom", "dataType": "String"}, {"name": "DynMoTTipoPrec", "dataType": "String"}, {"name": "DynMoTTipoReg", "dataType": "String"}, {"name": "DynMoTUsrAltVlr", "dataType": "String"}, {"name": "DynMoTVN", "dataType": "Decimal"}, {"name": "DynMoTVlrEspNom", "dataType": "String"}, {"name": "DynMoTVlrId", "dataType": "String"}, {"name": "DynMoTVlrNom", "dataType": "String"}, {"name": "DynMotReqVal", "dataType": "Decimal"}, {"name": "cotizacion_usd", "dataType": "Double"}, {"name": "moneda_movimiento", "dataType": "String"}, {"name": "mueve_efectivo", "dataType": "Boolean"}, {"name": "mueve_nominales", "dataType": "Boolean"}]', 
        '[{"name": "AAAAMM", "dataType": "String"}, {"name": "DynMoTBroNom", "dataType": "String"}, {"name": "DynMoTCatValor", "dataType": "Decimal"}, {"name": "DynMoTCtaDep", "dataType": "String"}, {"name": "DynMoTCtdId", "dataType": "Integer"}, {"name": "DynMoTCtraVlr", "dataType": "Decimal"}, {"name": "DynMoTDesc", "dataType": "String"}, {"name": "DynMoTDesc2", "dataType": "String"}, {"name": "DynMoTEstSis", "dataType": "String"}, {"name": "DynMoTFecOpe", "dataType": "Timestamp"}, {"name": "DynMoTFecVal", "dataType": "Timestamp"}, {"name": "DynMoTFormu", "dataType": "String"}, {"name": "DynMoTFunId", "dataType": "Integer"}, {"name": "DynMoTImpoMEfe", "dataType": "Decimal"}, {"name": "DynMoTMEfe", "dataType": "String"}, {"name": "DynMoTMVN", "dataType": "String"}, {"name": "DynMoTMonEfe", "dataType": "Integer"}, {"name": "DynMoTMonVlr", "dataType": "Integer"}, {"name": "DynMoTOpiId", "dataType": "Decimal"}, {"name": "DynMoTPrecio", "dataType": "Decimal"}, {"name": "DynMoTPrtNom", "dataType": "String"}, {"name": "DynMoTTipoPrec", "dataType": "String"}, {"name": "DynMoTTipoReg", "dataType": "String"}, {"name": "DynMoTUsrAltVlr", "dataType": "String"}, {"name": "DynMoTVN", "dataType": "Decimal"}, {"name": "DynMoTVlrEspNom", "dataType": "String"}, {"name": "DynMoTVlrId", "dataType": "String"}, {"name": "DynMoTVlrNom", "dataType": "String"}, {"name": "DynMotReqVal", "dataType": "Decimal"}, {"name": "moneda_movimiento", "dataType": "String"}, {"name": "mueve_efectivo", "dataType": "Boolean"}, {"name": "mueve_nominales", "dataType": "Boolean"}]', 
        '[{"name": "AAAAMM", "dataType": "String"}, {"name": "DynMoTBroNom", "dataType": "String"}, {"name": "DynMoTCatValor", "dataType": "Decimal"}, {"name": "DynMoTCtaDep", "dataType": "String"}, {"name": "DynMoTCtdId", "dataType": "Integer"}, {"name": "DynMoTCtraVlr", "dataType": "Decimal"}, {"name": "DynMoTDesc", "dataType": "String"}, {"name": "DynMoTDesc2", "dataType": "String"}, {"name": "DynMoTEstSis", "dataType": "String"}, {"name": "DynMoTFecOpe", "dataType": "Timestamp"}, {"name": "DynMoTFecVal", "dataType": "Timestamp"}, {"name": "DynMoTFormu", "dataType": "String"}, {"name": "DynMoTFunId", "dataType": "Integer"}, {"name": "DynMoTImpoMEfe", "dataType": "Decimal"}, {"name": "DynMoTMEfe", "dataType": "String"}, {"name": "DynMoTMVN", "dataType": "String"}, {"name": "DynMoTMonEfe", "dataType": "Integer"}, {"name": "DynMoTMonVlr", "dataType": "Integer"}, {"name": "DynMoTOpiId", "dataType": "Decimal"}, {"name": "DynMoTPrecio", "dataType": "Decimal"}, {"name": "DynMoTPrtNom", "dataType": "String"}, {"name": "DynMoTTipoPrec", "dataType": "String"}, {"name": "DynMoTTipoReg", "dataType": "String"}, {"name": "DynMoTUsrAltVlr", "dataType": "String"}, {"name": "DynMoTVN", "dataType": "Decimal"}, {"name": "DynMoTVlrEspNom", "dataType": "String"}, {"name": "DynMoTVlrId", "dataType": "String"}, {"name": "DynMoTVlrNom", "dataType": "String"}, {"name": "DynMotReqVal", "dataType": "Decimal"}, {"name": "cotizacion_usd", "dataType": "Double"}, {"name": "moneda_movimiento", "dataType": "String"}, {"name": "mueve_efectivo", "dataType": "Boolean"}, {"name": "mueve_nominales", "dataType": "Boolean"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_348_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((CAST(DynMoTTipoReg AS string) IN ('C', 'REVV')) AS BOOLEAN)
          THEN ((DynMoTImpoMEfe * -1) - DynMoTCtraVlr)
        WHEN CAST((CAST(DynMoTTipoReg AS string) IN ('V', 'REVC')) AS BOOLEAN)
          THEN (DynMoTCtraVlr - DynMoTImpoMEfe)
        ELSE NULL
      END
    ) AS DOUBLE) AS ganancia_moneda_origen,
    *
  
  FROM Union_363 AS in0

),

Formula_348_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST(((ganancia_moneda_origen IS NULL) OR ((LENGTH(ganancia_moneda_origen)) = 0)) AS BOOLEAN)
          THEN NULL
        WHEN CAST((ganancia_moneda_origen = 0) AS BOOLEAN)
          THEN 0
        WHEN CAST((DynMoTCtraVlr = 0) AS BOOLEAN)
          THEN 0
        ELSE ((ganancia_moneda_origen / DynMoTCtraVlr) * 100)
      END
    ) AS DOUBLE) AS ganancia_porcentual,
    CAST((ganancia_moneda_origen * cotizacion_usd) AS DOUBLE) AS ganancia_arbitrada_dolares,
    *
  
  FROM Formula_348_0 AS in0

),

Formula_358_0 AS (

  SELECT 
    CAST(0 AS DOUBLE) AS ganancia_moneda_origen,
    CAST(0 AS DOUBLE) AS ganancia_porcentual,
    CAST(0 AS DOUBLE) AS ganancia_arbitrada_dolares,
    *
  
  FROM Filter_352 AS in0

),

Union_359 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_348_1', 'Formula_358_0'], 
      [
        '[{"name": "ganancia_porcentual", "dataType": "Double"}, {"name": "ganancia_arbitrada_dolares", "dataType": "Double"}, {"name": "ganancia_moneda_origen", "dataType": "Double"}, {"name": "AAAAMM", "dataType": "String"}, {"name": "DynMoTBroNom", "dataType": "String"}, {"name": "DynMoTCatValor", "dataType": "Decimal"}, {"name": "DynMoTCtaDep", "dataType": "String"}, {"name": "DynMoTCtdId", "dataType": "Integer"}, {"name": "DynMoTCtraVlr", "dataType": "Decimal"}, {"name": "DynMoTDesc", "dataType": "String"}, {"name": "DynMoTDesc2", "dataType": "String"}, {"name": "DynMoTEstSis", "dataType": "String"}, {"name": "DynMoTFecOpe", "dataType": "Timestamp"}, {"name": "DynMoTFecVal", "dataType": "Timestamp"}, {"name": "DynMoTFormu", "dataType": "String"}, {"name": "DynMoTFunId", "dataType": "Integer"}, {"name": "DynMoTImpoMEfe", "dataType": "Decimal"}, {"name": "DynMoTMEfe", "dataType": "String"}, {"name": "DynMoTMVN", "dataType": "String"}, {"name": "DynMoTMonEfe", "dataType": "Integer"}, {"name": "DynMoTMonVlr", "dataType": "Integer"}, {"name": "DynMoTOpiId", "dataType": "Decimal"}, {"name": "DynMoTPrecio", "dataType": "Decimal"}, {"name": "DynMoTPrtNom", "dataType": "String"}, {"name": "DynMoTTipoPrec", "dataType": "String"}, {"name": "DynMoTTipoReg", "dataType": "String"}, {"name": "DynMoTUsrAltVlr", "dataType": "String"}, {"name": "DynMoTVN", "dataType": "Decimal"}, {"name": "DynMoTVlrEspNom", "dataType": "String"}, {"name": "DynMoTVlrId", "dataType": "String"}, {"name": "DynMoTVlrNom", "dataType": "String"}, {"name": "DynMotReqVal", "dataType": "Decimal"}, {"name": "cotizacion_usd", "dataType": "Double"}, {"name": "moneda_movimiento", "dataType": "String"}, {"name": "mueve_efectivo", "dataType": "Boolean"}, {"name": "mueve_nominales", "dataType": "Boolean"}]', 
        '[{"name": "ganancia_moneda_origen", "dataType": "Double"}, {"name": "ganancia_porcentual", "dataType": "Double"}, {"name": "ganancia_arbitrada_dolares", "dataType": "Double"}, {"name": "mueve_efectivo", "dataType": "Boolean"}, {"name": "mueve_nominales", "dataType": "Boolean"}, {"name": "AAAAMM", "dataType": "String"}, {"name": "DynMoTFecVal", "dataType": "Timestamp"}, {"name": "DynMoTFecOpe", "dataType": "Timestamp"}, {"name": "DynMoTMonEfe", "dataType": "Integer"}, {"name": "data_date_part", "dataType": "Integer"}, {"name": "DynMoTId", "dataType": "Decimal"}, {"name": "DynMoTTipoReg", "dataType": "String"}, {"name": "DynMoTPrtNom", "dataType": "String"}, {"name": "DynMoTMEfe", "dataType": "String"}, {"name": "DynMoTMVN", "dataType": "String"}, {"name": "DynMoTCtdId", "dataType": "Integer"}, {"name": "DynMoTFunId", "dataType": "Integer"}, {"name": "DynMoTFunNom", "dataType": "String"}, {"name": "DynMoTFunIdCli", "dataType": "Integer"}, {"name": "DynMoTDesc", "dataType": "String"}, {"name": "DynMoTDesc2", "dataType": "String"}, {"name": "DynMoTOpiId", "dataType": "Decimal"}, {"name": "DynMoTOrdinal", "dataType": "Decimal"}, {"name": "DynMoTVlrId", "dataType": "String"}, {"name": "DynMoTMonVlr", "dataType": "Integer"}, {"name": "DynMoTVlrNom", "dataType": "String"}, {"name": "DynMoTVlrVto", "dataType": "Timestamp"}, {"name": "DynMoTCodAgata", "dataType": "String"}, {"name": "DynMoTTS", "dataType": "String"}, {"name": "DynMoTVN", "dataType": "Decimal"}, {"name": "DynMoTPrecio", "dataType": "Decimal"}, {"name": "DynMoTTipoPrec", "dataType": "String"}, {"name": "DynMoTPrTesCSpread", "dataType": "Decimal"}, {"name": "DynMoTPrTesSSpread", "dataType": "Decimal"}, {"name": "DynMoTIVAUSD", "dataType": "Decimal"}, {"name": "DynMoTCapCalcMefe", "dataType": "Decimal"}, {"name": "DynMoTBroId", "dataType": "Integer"}, {"name": "DynMoTBroNom", "dataType": "String"}, {"name": "DynMoTBroCtr", "dataType": "Integer"}, {"name": "DynMoTBroCtrNom", "dataType": "String"}, {"name": "DynMoTGanMEfe", "dataType": "Decimal"}, {"name": "DynMoTGanUSD", "dataType": "Decimal"}, {"name": "DynMoTIVAMEfe", "dataType": "Decimal"}, {"name": "DynMoTIntMEfe", "dataType": "Decimal"}, {"name": "DynMoTGastoMEfe", "dataType": "Decimal"}, {"name": "DynMoTGastoUSD", "dataType": "Decimal"}, {"name": "DynMoTDevIVAMEfe", "dataType": "Decimal"}, {"name": "DynMoTDevIVAUSD", "dataType": "Decimal"}, {"name": "DynMoTIRPFMEfe", "dataType": "Decimal"}, {"name": "DynMoTIRPFUSD", "dataType": "Decimal"}, {"name": "DynMoTImpuMEfe", "dataType": "Decimal"}, {"name": "DynMoTImpuUSD", "dataType": "Decimal"}, {"name": "DynMoTFecAlt", "dataType": "Timestamp"}, {"name": "DynMoTUsrAlt", "dataType": "String"}, {"name": "DynMoTCtaDep", "dataType": "String"}, {"name": "DynMoTRes", "dataType": "String"}, {"name": "DynMoTLoteDW", "dataType": "Decimal"}, {"name": "DynMoTEstSis", "dataType": "String"}, {"name": "DynMoTOpmId", "dataType": "Decimal"}, {"name": "DynMoTLoteOri", "dataType": "Decimal"}, {"name": "DynMoTEstado", "dataType": "String"}, {"name": "DynMoTHash", "dataType": "String"}, {"name": "DynMoTIntUSD", "dataType": "Decimal"}, {"name": "DynMoTImpoMEfe", "dataType": "Decimal"}, {"name": "DynMoTImpoUSD", "dataType": "Decimal"}, {"name": "DynMoTCapCalcMefeUSD", "dataType": "Decimal"}, {"name": "DynMoTUsrAltVlr", "dataType": "String"}, {"name": "DynMotReqVal", "dataType": "Decimal"}, {"name": "DynMoTCatValor", "dataType": "Decimal"}, {"name": "DynMoTFormu", "dataType": "String"}, {"name": "DynMoTVlrEspNom", "dataType": "String"}, {"name": "DynMoTVlrEspId", "dataType": "String"}, {"name": "DynMoTVlrIsinCode", "dataType": "String"}, {"name": "DynMoTCtraVlr", "dataType": "Decimal"}, {"name": "DynMoTIntCalc", "dataType": "Decimal"}, {"name": "DynMoTCtdNom", "dataType": "String"}, {"name": "DynMoTValUSD", "dataType": "Decimal"}, {"name": "DynMoTTipoReg2", "dataType": "String"}, {"name": "DynMotPSPrec", "dataType": "Decimal"}, {"name": "DynMoTSisFecAlt", "dataType": "Timestamp"}, {"name": "DynMoTNroCont", "dataType": "String"}, {"name": "data_timestamp_part", "dataType": "Timestamp"}, {"name": "moneda_movimiento", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_364_0 AS (

  SELECT 
    CAST((DynMoTImpoMEfe * cotizacion_usd) AS DOUBLE) AS efectivo_arbitrado_dolares,
    *
  
  FROM Union_359 AS in0

),

AlteryxSelect_347 AS (

  SELECT 
    AAAAMM AS AAAAMM,
    DynMoTFecOpe AS fecha_operacion,
    DynMoTFecVal AS fecha_liquidacion,
    CAST(DynMoTCtdId AS string) AS numero_contrato_inversiones,
    CAST(DynMoTOpiId AS INTEGER) AS id_movimiento,
    DynMoTTipoReg AS codigo_tipo_movimiento,
    DynMoTPrtNom AS descripcion_tipo_movimiento,
    mueve_efectivo AS mueve_efectivo,
    mueve_nominales AS mueve_nominales,
    DynMoTVlrId AS codigo_valor,
    DynMoTVlrNom AS nombre_valor,
    DynMoTVlrEspNom AS descripcion_especie_valor,
    CAST(moneda_movimiento AS string) AS moneda_movimiento,
    DynMoTVN AS movimiento_nominal,
    DynMoTPrecio AS precio,
    DynMoTTipoPrec AS tipo_precio,
    DynMoTImpoMEfe AS movimiento_efectivo,
    DynMoTCtraVlr AS movimiento_efectivo_contraparte,
    efectivo_arbitrado_dolares AS efectivo_arbitrado_dolares,
    ganancia_moneda_origen AS ganancia_moneda_origen,
    ganancia_porcentual AS ganancia_porcentual,
    ganancia_arbitrada_dolares AS ganancia_arbitrada_dolares,
    DynMoTBroNom AS contraparte,
    DynMoTCtaDep AS cuenta_liquidacion,
    DynMoTEstSis AS codigo_estado_movimiento,
    DynMotReqVal AS req_porcentaje,
    CAST(DynMoTCatValor AS string) AS req_categoria_valor,
    DynMoTFormu AS formulario,
    DynMoTFunId AS codigo_funcionario_operacion,
    DynMoTUsrAltVlr AS usuario_movimiento,
    DynMoTDesc AS descripcion_movimiento_1,
    DynMoTDesc2 AS descripcion_movimiento_2
  
  FROM Formula_364_0 AS in0

),

Join_328_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`numero_contrato_inversiones`, 
    `es_primer_titular`, 
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
    `AAAAMM`)
  
  FROM AlteryxSelect_347 AS in0
  INNER JOIN Filter_327 AS in1
     ON (in0.numero_contrato_inversiones = in1.numero_contrato_inversiones)

),

AlteryxSelect_369 AS (

  SELECT 
    AAAAMM AS AAAAMM,
    idf_pers_ods AS idf_pers_ods,
    fecha_operacion AS fecha_operacion,
    fecha_liquidacion AS fecha_liquidacion,
    numero_contrato_inversiones AS numero_contrato_inversiones,
    id_movimiento AS id_movimiento,
    codigo_tipo_movimiento AS codigo_tipo_movimiento,
    descripcion_tipo_movimiento AS descripcion_tipo_movimiento,
    mueve_efectivo AS mueve_efectivo,
    mueve_nominales AS mueve_nominales,
    codigo_valor AS codigo_valor,
    nombre_valor AS nombre_valor,
    descripcion_especie_valor AS descripcion_especie_valor,
    moneda_movimiento AS moneda_movimiento,
    movimiento_nominal AS movimiento_nominal,
    precio AS precio,
    tipo_precio AS tipo_precio,
    movimiento_efectivo AS movimiento_efectivo,
    movimiento_efectivo_contraparte AS movimiento_efectivo_contraparte,
    efectivo_arbitrado_dolares AS efectivo_arbitrado_dolares,
    ganancia_moneda_origen AS ganancia_moneda_origen,
    ganancia_porcentual AS ganancia_porcentual,
    ganancia_arbitrada_dolares AS ganancia_arbitrada_dolares,
    contraparte AS contraparte,
    cuenta_liquidacion AS cuenta_liquidacion,
    codigo_estado_movimiento AS codigo_estado_movimiento,
    req_porcentaje AS req_porcentaje,
    req_categoria_valor AS req_categoria_valor,
    formulario AS formulario,
    codigo_funcionario_operacion AS codigo_funcionario_operacion,
    usuario_movimiento AS usuario_movimiento,
    descripcion_movimiento_1 AS descripcion_movimiento_1,
    descripcion_movimiento_2 AS descripcion_movimiento_2
  
  FROM Join_328_inner AS in0

),

cast_productos_inversiones_transacciones AS (

  {#Processes and formats investment transaction data for detailed financial analysis.#}
  SELECT 
    {{ var('ref_data_date_part') }} AS ref_data_date_part,
    current_timestamp() AS ref_timestamp_procesamiento,
    AAAAMM AS AAAAMM,
    idf_pers_ods AS idf_pers_ods,
    CAST(fecha_operacion AS DATE) AS fecha_operacion,
    CAST(fecha_liquidacion AS DATE) AS fecha_liquidacion,
    numero_contrato_inversiones AS numero_contrato_inversiones,
    CAST(id_movimiento AS BIGINT) AS id_movimiento,
    codigo_tipo_movimiento AS codigo_tipo_movimiento,
    descripcion_tipo_movimiento AS descripcion_tipo_movimiento,
    mueve_efectivo AS mueve_efectivo,
    mueve_nominales AS mueve_nominales,
    codigo_valor AS codigo_valor,
    nombre_valor AS nombre_valor,
    descripcion_especie_valor AS descripcion_especie_valor,
    moneda_movimiento AS moneda_movimiento,
    CAST(movimiento_nominal AS DECIMAL (19, 5)) AS movimiento_nominal,
    CAST(precio AS DECIMAL (15, 6)) AS precio,
    tipo_precio AS tipo_precio,
    CAST(movimiento_efectivo AS DECIMAL (19, 5)) AS movimiento_efectivo,
    CAST(movimiento_efectivo_contraparte AS DECIMAL (19, 5)) AS movimiento_efectivo_contraparte,
    CAST(efectivo_arbitrado_dolares AS DECIMAL (19, 2)) AS efectivo_arbitrado_dolares,
    CAST(ganancia_moneda_origen AS DECIMAL (19, 2)) AS ganancia_moneda_origen,
    CAST(ganancia_porcentual AS DECIMAL (19, 2)) AS ganancia_porcentual,
    CAST(ganancia_arbitrada_dolares AS DECIMAL (19, 2)) AS ganancia_arbitrada_dolares,
    contraparte AS contraparte,
    cuenta_liquidacion AS cuenta_liquidacion,
    codigo_estado_movimiento AS codigo_estado_movimiento,
    CAST(req_porcentaje AS DECIMAL (10, 5)) AS req_porcentaje,
    req_categoria_valor AS req_categoria_valor,
    formulario AS formulario,
    codigo_funcionario_operacion AS codigo_funcionario_operacion,
    usuario_movimiento AS usuario_movimiento,
    descripcion_movimiento_1 AS descripcion_movimiento_1,
    descripcion_movimiento_2 AS descripcion_movimiento_2
  
  FROM AlteryxSelect_369 AS in0

)

SELECT *

FROM cast_productos_inversiones_transacciones
