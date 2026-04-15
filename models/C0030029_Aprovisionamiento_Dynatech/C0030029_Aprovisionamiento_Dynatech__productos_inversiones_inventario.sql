{{
  config({    
    "materialized": "incremental",
    "alias": "productos_inversiones_inventario",
    "database": "dev_business",
    "incremental_strategy": "delete+insert",
    "schema": "productos",
    "unique_key": ["ref_data_date_part"]
  })
}}

WITH AlteryxSelect_65 AS (

  SELECT *
  
  FROM {{ ref('C0030029_Aprovisionamiento_Dynatech__AlteryxSelect_65')}}

),

dbo_dynclipos AS (

  SELECT * 
  
  FROM {{ source('dev_curated_uyasdbtest_dynvaldwsantander', 'dbo_dynclipos') }}

),

fr_rf_dcl AS (

  {#Filters dynamic clip positions based on a specific date part for targeted analysis.#}
  SELECT * 
  
  FROM dbo_dynclipos AS in0
  
  WHERE {{ var('ref_data_date_part') }} == data_date_part

),

Formula_379_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((DynCPoMonSmb = 'EURO') AS BOOLEAN)
          THEN 'EUR'
        ELSE DynCPoMonSmb
      END
    ) AS string) AS DynCPoMonSmb,
    * EXCEPT (`dyncpomonsmb`)
  
  FROM fr_rf_dcl AS in0

),

AlteryxSelect_88 AS (

  SELECT 
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynCPoFecha AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynCPoFecha AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynCPoFecha AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynCPoFecha AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(DynCPoFecha AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_dato,
    CAST(DynCPoCtdId AS string) AS numero_contrato_inversiones,
    DynCPoVlrID AS codigo_valor,
    DynCPoVN AS valor_nominal,
    CAST(DynCPoMonSmb AS string) AS moneda_valor,
    DynCPoPrec AS precio_valor,
    DynCPoSalValMO AS valor_efectivo_moneda_origen,
    DynCPoSalValDol AS valor_efectivo_arbitrado_dolares,
    DynCPoIntCorrMO AS interes_corrido_moneda_origen,
    DynCPoIntCorDol AS interes_corrido_arbitrado_dolares,
    DynCPoEstado AS estado_valor,
    DynCPoVlrTipoPrec AS tipo_precio,
    DynCPoVlrTipoIns AS codigo_tipo_valor,
    DynCPoVlrNom AS nombre_valor,
    DynCPoSaldoEnGar AS saldo_en_garantia,
    DynCPoVlrEspId AS codigo_especie_valor,
    DynCPoVlrFecVto AS fecha_vencimiento_valor,
    DynCPoCRMPPP AS precio_promedio_ponderado,
    DynCPoCRMComplejo AS req_complejidad,
    DynCPoCRMAUM AS req_aum,
    DynCPoCRMLiquidez AS req_liquidez,
    DynCPoCRMPlazo AS req_plazo,
    DynCPoCRMPorcReq AS req_porcentaje,
    DynCPoCRMCatValor AS req_categoria_valor,
    CAST(DynCPoCliNroCont AS string) AS numero_contrato
  
  FROM Formula_379_0 AS in0

),

he0_dt_ini_fin_habil_mes_prt AS (

  SELECT * 
  
  FROM {{ source('dev_curated.ods', 'he0_dt_ini_fin_habil_mes_prt') }}

),

fr_rf_habil AS (

  {#Filters records based on a specific date part for targeted analysis.#}
  SELECT * 
  
  FROM he0_dt_ini_fin_habil_mes_prt AS in0
  
  WHERE data_date_part == {{ var('ref_data_date_part') }}

),

Formula_334_0 AS (

  {#Converts and includes date information for data records.#}
  SELECT 
    TO_DATE(CAST(FEC_DATA AS STRING), 'yyyyMMdd') AS fecha_dato,
    *
  
  FROM fr_rf_habil AS in0

),

Unique_340 AS (

  SELECT 
    (ROW_NUMBER() OVER (PARTITION BY fecha_dato ORDER BY fecha_dato NULLS FIRST)) AS row_number,
    *
  
  FROM Formula_334_0 AS in0

),

Unique_340_filter AS (

  SELECT * 
  
  FROM Unique_340 AS in0
  
  WHERE (row_number = 1)

),

Unique_340_drop_0 AS (

  SELECT * EXCEPT (`row_number`)
  
  FROM Unique_340_filter AS in0

),

Join_335_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`FEC_DATA`, `fecha_dato`)
  
  FROM AlteryxSelect_88 AS in0
  INNER JOIN Unique_340_drop_0 AS in1
     ON (CAST(in0.fecha_dato AS DATE) = in1.fecha_dato)

),

Formula_95_0 AS (

  SELECT 
    CAST(TRIM(codigo_valor) AS string) AS codigo_valor,
    CAST(((year(fecha_dato) * 100) + month(fecha_dato)) AS STRING) AS AAAAMM,
    * EXCEPT (`codigo_valor`)
  
  FROM Join_335_inner AS in0

),

AlteryxSelect_148 AS (

  SELECT 
    AAAAMM AS AAAAMM,
    fecha_dato AS fecha_dato,
    numero_contrato_inversiones AS numero_contrato_inversiones,
    codigo_valor AS codigo_valor,
    valor_nominal AS valor_nominal,
    moneda_valor AS moneda_valor,
    precio_valor AS precio_valor,
    valor_efectivo_moneda_origen AS valor_efectivo_moneda_origen,
    valor_efectivo_arbitrado_dolares AS valor_efectivo_arbitrado_dolares,
    interes_corrido_moneda_origen AS interes_corrido_moneda_origen,
    interes_corrido_arbitrado_dolares AS interes_corrido_arbitrado_dolares,
    estado_valor AS estado_valor,
    tipo_precio AS tipo_precio,
    codigo_tipo_valor AS codigo_tipo_valor,
    nombre_valor AS nombre_valor,
    saldo_en_garantia AS saldo_en_garantia,
    codigo_especie_valor AS codigo_especie_valor,
    fecha_vencimiento_valor AS fecha_vencimiento_valor,
    precio_promedio_ponderado AS precio_promedio_ponderado,
    req_complejidad AS req_complejidad,
    req_aum AS req_aum,
    req_liquidez AS req_liquidez,
    req_plazo AS req_plazo,
    req_porcentaje AS req_porcentaje,
    req_categoria_valor AS req_categoria_valor,
    numero_contrato AS numero_contrato,
    * EXCEPT (`AAAAMM`, 
    `fecha_dato`, 
    `numero_contrato_inversiones`, 
    `codigo_valor`, 
    `valor_nominal`, 
    `moneda_valor`, 
    `precio_valor`, 
    `valor_efectivo_moneda_origen`, 
    `valor_efectivo_arbitrado_dolares`, 
    `interes_corrido_moneda_origen`, 
    `interes_corrido_arbitrado_dolares`, 
    `estado_valor`, 
    `tipo_precio`, 
    `codigo_tipo_valor`, 
    `nombre_valor`, 
    `saldo_en_garantia`, 
    `codigo_especie_valor`, 
    `fecha_vencimiento_valor`, 
    `precio_promedio_ponderado`, 
    `req_complejidad`, 
    `req_aum`, 
    `req_liquidez`, 
    `req_plazo`, 
    `req_porcentaje`, 
    `req_categoria_valor`, 
    `numero_contrato`)
  
  FROM Formula_95_0 AS in0

),

Filter_92 AS (

  SELECT * 
  
  FROM AlteryxSelect_148 AS in0
  
  WHERE (CAST(valor_nominal AS DECIMAL (19, 9)) > CAST('0' AS DECIMAL (19, 9)))

),

Summarize_155 AS (

  SELECT 
    MAX(fecha_dato) AS Max_fecha_dato,
    AAAAMM AS AAAAMM
  
  FROM Filter_92 AS in0
  
  GROUP BY AAAAMM

),

Join_162_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`AAAAMM`, `Max_fecha_dato`)
  
  FROM Filter_92 AS in0
  INNER JOIN Summarize_155 AS in1
     ON ((in0.fecha_dato = in1.Max_fecha_dato) AND (in0.AAAAMM = in1.AAAAMM))

),

Filter_332 AS (

  {#Selects primary account holders from the dataset.#}
  SELECT * 
  
  FROM AlteryxSelect_65 AS in0
  
  WHERE es_primer_titular = 1

),

Join_331_inner AS (

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
  
  FROM Join_162_inner AS in0
  INNER JOIN Filter_332 AS in1
     ON (in0.numero_contrato_inversiones = in1.numero_contrato_inversiones)

),

AlteryxSelect_170 AS (

  SELECT 
    AAAAMM AS AAAAMM,
    fecha_dato AS fecha_dato,
    numero_contrato_inversiones AS numero_contrato_inversiones,
    idf_pers_ods AS idf_pers_ods,
    codigo_valor AS codigo_valor,
    valor_nominal AS valor_nominal,
    moneda_valor AS moneda_valor,
    precio_valor AS precio_valor,
    valor_efectivo_moneda_origen AS valor_efectivo_moneda_origen,
    valor_efectivo_arbitrado_dolares AS valor_efectivo_arbitrado_dolares,
    interes_corrido_moneda_origen AS interes_corrido_moneda_origen,
    interes_corrido_arbitrado_dolares AS interes_corrido_arbitrado_dolares,
    estado_valor AS estado_valor,
    tipo_precio AS tipo_precio,
    codigo_tipo_valor AS codigo_tipo_valor,
    nombre_valor AS nombre_valor,
    saldo_en_garantia AS saldo_en_garantia,
    codigo_especie_valor AS codigo_especie_valor,
    fecha_vencimiento_valor AS fecha_vencimiento_valor,
    precio_promedio_ponderado AS precio_promedio_ponderado,
    req_complejidad AS req_complejidad,
    req_aum AS req_aum,
    req_liquidez AS req_liquidez,
    req_plazo AS req_plazo,
    req_porcentaje AS req_porcentaje,
    req_categoria_valor AS req_categoria_valor,
    numero_contrato AS numero_contrato
  
  FROM Join_331_inner AS in0

),

cast_productos_inversiones_inventario AS (

  {#Processes and formats investment contract inventory data for reporting and analysis.#}
  SELECT 
    {{ var('ref_data_date_part') }} AS ref_data_date_part,
    current_timestamp() AS ref_timestamp_procesamiento,
    AAAAMM AS AAAAMM,
    CAST(fecha_dato AS DATE) AS fecha_dato,
    numero_contrato_inversiones AS numero_contrato_inversiones,
    idf_pers_ods AS idf_pers_ods,
    codigo_valor AS codigo_valor,
    CAST(valor_nominal AS DECIMAL (23, 5)) AS valor_nominal,
    moneda_valor AS moneda_valor,
    CAST(precio_valor AS DECIMAL (19, 5)) AS precio_valor,
    CAST(valor_efectivo_moneda_origen AS DECIMAL (23, 5)) AS valor_efectivo_moneda_origen,
    CAST(valor_efectivo_arbitrado_dolares AS DECIMAL (19, 5)) AS valor_efectivo_arbitrado_dolares,
    CAST(interes_corrido_moneda_origen AS DECIMAL (19, 5)) AS interes_corrido_moneda_origen,
    CAST(interes_corrido_arbitrado_dolares AS DECIMAL (19, 5)) AS interes_corrido_arbitrado_dolares,
    estado_valor AS estado_valor,
    tipo_precio AS tipo_precio,
    codigo_tipo_valor AS codigo_tipo_valor,
    nombre_valor AS nombre_valor,
    CAST(saldo_en_garantia AS DECIMAL (19, 5)) AS saldo_en_garantia,
    codigo_especie_valor AS codigo_especie_valor,
    CAST(fecha_vencimiento_valor AS date) AS fecha_vencimiento_valor,
    CAST(precio_promedio_ponderado AS DECIMAL (15, 6)) AS precio_promedio_ponderado,
    req_complejidad AS req_complejidad,
    req_aum AS req_aum,
    req_liquidez AS req_liquidez,
    req_plazo AS req_plazo,
    CAST(req_porcentaje AS DECIMAL (10, 5)) AS req_porcentaje,
    req_categoria_valor AS req_categoria_valor,
    numero_contrato AS numero_contrato
  
  FROM AlteryxSelect_170 AS in0

)

SELECT *

FROM cast_productos_inversiones_inventario
