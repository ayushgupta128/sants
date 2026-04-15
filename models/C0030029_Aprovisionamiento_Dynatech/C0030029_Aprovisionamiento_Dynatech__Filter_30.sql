{{
  config({    
    "materialized": "ephemeral",
    "database": "dev_ref_control",
    "schema": "prophecy_tmp"
  })
}}

WITH DynClientesCuentasAPP AS (

  SELECT * 
  
  FROM {{ source('dev_curated_uyasdbtest_dynvaldwsantander', 'dbo_dynclientescuentasapp') }}

),

Formula_34_0 AS (

  SELECT 
    CAST((SUBSTRING(DynCliCueACuentaId, (((LENGTH(DynCliCueACuentaId)) - 12) + 1), 12)) AS string) AS numero_contrato,
    CAST((SUBSTRING((SUBSTRING(DynCliCueACuentaId, (((LENGTH(DynCliCueACuentaId)) - 16) + 1), 16)), 1, 4)) AS string) AS codigo_sucursal_cuenta,
    CAST((
      CASE
        WHEN CAST((DynCliCueAMonSmb = 'EURO') AS BOOLEAN)
          THEN 'EUR'
        ELSE DynCliCueAMonSmb
      END
    ) AS string) AS DynCliCueAMonSmb,
    * EXCEPT (`dynclicueamonsmb`)
  
  FROM DynClientesCuentasAPP AS in0

),

AlteryxSelect_31 AS (

  SELECT 
    CAST(DynCliCueACtdId AS string) AS numero_contrato_inversiones,
    DynCliCueAProId AS tipo_producto,
    DynCliCueASucId AS DynCliCueASucId,
    codigo_sucursal_cuenta AS codigo_sucursal_cuenta,
    numero_contrato AS numero_contrato,
    DynCliCueAMonSmb AS moneda_contrato,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynCliCueAFecAlt AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynCliCueAFecAlt AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(DynCliCueAFecAlt AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(DynCliCueAFecAlt AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(DynCliCueAFecAlt AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_alta_relacion_cuenta_contrato,
    DynCliCueAEstado AS estado_cuenta,
    * EXCEPT (`DynCliCueACuentaId`, 
    `DynCliCueASucId`, 
    `codigo_sucursal_cuenta`, 
    `numero_contrato`, 
    `DynCliCueACtdId`, 
    `DynCliCueAProId`, 
    `DynCliCueAMonSmb`, 
    `DynCliCueAFecAlt`, 
    `DynCliCueAEstado`)
  
  FROM Formula_34_0 AS in0

),

Filter_30 AS (

  SELECT * 
  
  FROM AlteryxSelect_31 AS in0
  
  WHERE (estado_cuenta = 'HAB')

)

SELECT *

FROM Filter_30
