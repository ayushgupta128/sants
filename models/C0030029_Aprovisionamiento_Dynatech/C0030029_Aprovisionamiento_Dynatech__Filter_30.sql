{{
  config({    
    "materialized": "ephemeral",
    "database": "akash_demos",
    "schema": "demos"
  })
}}

WITH dbo_dynclientescuentasapp_1 AS (

  {#VisualGroup: Tabla_cuentas_cash_asociadas_a_contratos_dynatech#}
  {#Loads client account data from the akash_demos_demos source for further processing.#}
  SELECT * 
  
  FROM {{ source('akash_demos_demos', 'dbo_dynclientescuentasapp') }}

),

Formula_34_0 AS (

  {#VisualGroup: Tabla_cuentas_cash_asociadas_a_contratos_dynatech#}
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
  
  FROM dbo_dynclientescuentasapp_1 AS in0

),

AlteryxSelect_31 AS (

  {#VisualGroup: Tabla_cuentas_cash_asociadas_a_contratos_dynatech#}
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

  {#VisualGroup: Tabla_cuentas_cash_asociadas_a_contratos_dynatech#}
  SELECT * 
  
  FROM AlteryxSelect_31 AS in0
  
  WHERE (estado_cuenta = 'HAB')

)

SELECT *

FROM Filter_30
