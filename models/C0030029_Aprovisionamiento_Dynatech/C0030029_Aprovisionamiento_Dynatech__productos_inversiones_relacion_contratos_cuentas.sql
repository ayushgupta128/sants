{{
  config({    
    "materialized": "incremental",
    "alias": "productos_inversiones_relacion_contratos_cuentas",
    "database": "dev_business",
    "incremental_strategy": "delete+insert",
    "schema": "productos",
    "unique_key": ["ref_data_date_part"]
  })
}}

WITH Filter_30 AS (

  SELECT *
  
  FROM {{ ref('C0030029_Aprovisionamiento_Dynatech__Filter_30')}}

),

AlteryxSelect_67 AS (

  {#VisualGroup: Tabla_cuentas_cash_asociadas_a_contratos_dynatech#}
  SELECT 
    numero_contrato_inversiones AS numero_contrato_inversiones,
    codigo_sucursal_cuenta AS codigo_sucursal_cuenta,
    numero_contrato AS numero_contrato,
    CAST(moneda_contrato AS string) AS moneda_contrato,
    fecha_alta_relacion_cuenta_contrato AS fecha_alta_relacion_cuenta_contrato,
    estado_cuenta AS estado_cuenta
  
  FROM Filter_30 AS in0

),

cast_productos_inversiones_relacion_contratos_cuentas AS (

  {#VisualGroup: Tabla_cuentas_cash_asociadas_a_contratos_dynatech#}
  {#Generates a report on investment contracts linked to account details, including contract numbers, branch codes, and account status.#}
  SELECT 
    {{ var('ref_data_date_part') }} AS ref_data_date_part,
    current_timestamp() AS ref_timestamp_procesamiento,
    numero_contrato_inversiones AS numero_contrato_inversiones,
    codigo_sucursal_cuenta AS codigo_sucursal_cuenta,
    numero_contrato AS numero_contrato,
    moneda_contrato AS moneda_contrato,
    CAST(fecha_alta_relacion_cuenta_contrato AS DATE) AS fecha_alta_relacion_cuenta_contrato,
    estado_cuenta AS estado_cuenta
  
  FROM AlteryxSelect_67 AS in0

)

{#VisualGroup: Tabla_cuentas_cash_asociadas_a_contratos_dynatech#}
{#Merges investment product data with contracts and accounts for incremental updates.#}
SELECT *

FROM cast_productos_inversiones_relacion_contratos_cuentas
