{{
  config({    
    "materialized": "table",
    "database": "akash_demos",
    "schema": "demos"
  })
}}

WITH dbo_dynclientestitularesapp_1 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  {#Loads client and account holder data from the Akash Demos database for further processing.#}
  SELECT * 
  
  FROM {{ source('akash_demos_demos', 'dbo_dynclientestitularesapp') }}

),

fr_rf_DynClientesTitularesAPP AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  {#Filters client data based on a specific date part for targeted analysis.#}
  SELECT * 
  
  FROM dbo_dynclientestitularesapp_1 AS in0
  
  WHERE {{ var('ref_data_date_part') }} == data_date_part

),

active_clients AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT * 
  
  FROM fr_rf_DynClientesTitularesAPP AS in0
  
  WHERE (
          NOT(
            DynCliTitAEstado = 'ANU')
        )

),

Formula_43_0 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    CAST((
      CONCAT(
        '10137', 
        (
          SUBSTRING(
            (
              CONCAT(
                '00000000', 
                (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(DynCliTitAPNumPer AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
            ), 
            (
              (
                (
                  LENGTH(
                    (
                      CONCAT(
                        '00000000', 
                        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(DynCliTitAPNumPer AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
                    ))
                )
                - 8
              )
              + 1
            ), 
            8)
        ))
    ) AS string) AS idf_pers_ods,
    *
  
  FROM active_clients AS in0

),

AlteryxSelect_44 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  {#Transforms and organizes investment contract data with relevant dates and statuses for compliance tracking.#}
  SELECT 
    idf_pers_ods AS idf_pers_ods,
    CAST(DynCliTitACtdId AS string) AS numero_contrato_inversiones,
    CAST(DynCliTitAFec AS DATE) AS fecha_dato,
    CAST(DynCliTitAFecAlt AS DATE) AS fecha_alta_titular,
    DynCliTitAEstado AS estado_persona,
    CAST(DynCliTitAW8FchIni AS date) AS fecha_inicio_w8,
    CAST(DynCliTitAW8FchVto AS date) AS fecha_vencimiento_w8,
    DynCliTitAW8EstSis AS estado_w8,
    CAST(DynCliTitAF3103FchIni AS date) AS fecha_inicio_3103,
    CAST(DynCliTitAF3103FchVto AS date) AS fecha_vencimiento_3103,
    DynCliTitAF3103EstSis AS estado_3103,
    CAST(DynCliTitAF306FchIni AS date) AS fecha_inicio_306,
    CAST(DynCliTitAF306FchVto AS DATE) AS fecha_vencimiento_306,
    DynCliTitAF306EstSis AS estado_306,
    * EXCEPT (`DynCliTitAPNumPer`, 
    `idf_pers_ods`, 
    `DynCliTitACtdId`, 
    `DynCliTitAFec`, 
    `DynCliTitAFecAlt`, 
    `DynCliTitAEstado`, 
    `DynCliTitAW8FchIni`, 
    `DynCliTitAW8FchVto`, 
    `DynCliTitAW8EstSis`, 
    `DynCliTitAF3103FchIni`, 
    `DynCliTitAF3103FchVto`, 
    `DynCliTitAF3103EstSis`, 
    `DynCliTitAF306FchIni`, 
    `DynCliTitAF306FchVto`, 
    `DynCliTitAF306EstSis`)
  
  FROM Formula_43_0 AS in0

),

productos_cuentas_inventario_1 AS (

  {#Loads inventory product account data from the akash_demos_demos source for further processing.#}
  SELECT * 
  
  FROM {{ source('akash_demos_demos', 'productos_cuentas_inventario') }}

),

fr_ref_pci AS (

  {#Extracts inventory product accounts matching a specific date part for reference.#}
  SELECT * 
  
  FROM productos_cuentas_inventario_1 AS in0
  
  WHERE {{ var('ref_data_date_part') }} == ref_data_date_part

),

AlteryxSelect_378 AS (

  {#Reorganizes account inventory data by renaming and excluding specific fields.#}
  SELECT 
    codigo_sucursal_cuenta AS codigo_sucursal_cuenta,
    moneda_cuenta AS moneda_contrato,
    * EXCEPT (codigo_sucursal_cuenta, moneda_cuenta)
  
  FROM fr_ref_pci AS in0

),

Filter_33 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  {#Identifies records where the titular status is marked as true.#}
  SELECT * 
  
  FROM AlteryxSelect_378 AS in0
  
  WHERE CAST(es_titular AS boolean) = true

),

Filter_30 AS (

  SELECT *
  
  FROM {{ ref('C0030029_Aprovisionamiento_Dynatech__Filter_30')}}

),

Join_35_inner AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    in1.moneda_contrato AS Right_moneda_contrato,
    in0.*,
    in1.* EXCEPT (`es_titular`, `numero_contrato`, `moneda_contrato`, `codigo_sucursal_cuenta`)
  
  FROM Filter_30 AS in0
  INNER JOIN Filter_33 AS in1
     ON (
      (
        (in0.codigo_sucursal_cuenta = in1.codigo_sucursal_cuenta)
        AND (in0.numero_contrato = in1.numero_contrato)
      )
      AND (in0.moneda_contrato = in1.moneda_contrato)
    )

),

Summarize_37 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    COUNT((
      CASE
        WHEN ((idf_pers_ods = NULL) OR (idf_pers_ods = ''))
          THEN 1
        ELSE NULL
      END
    )) AS Count,
    numero_contrato_inversiones AS numero_contrato_inversiones,
    idf_pers_ods AS idf_pers_ods
  
  FROM Join_35_inner AS in0
  
  GROUP BY 
    numero_contrato_inversiones, idf_pers_ods

),

Sample_39_recordId AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `id_39`
  
  FROM Summarize_37

),

Sample_39 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    (ROW_NUMBER() OVER (PARTITION BY numero_contrato_inversiones ORDER BY numero_contrato_inversiones NULLS FIRST)) AS row_number,
    *
  
  FROM Sample_39_recordId AS in0

),

Sample_39_filter AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT * 
  
  FROM Sample_39 AS in0
  
  WHERE (row_number <= 1)

),

Sample_39_drop_row_number_0 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT * EXCEPT (`id_39`, `row_number`)
  
  FROM Sample_39_filter AS in0

),

Join_48_inner AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`numero_contrato_inversiones`, `idf_pers_ods`, `Count`)
  
  FROM AlteryxSelect_44 AS in0
  INNER JOIN Sample_39_drop_row_number_0 AS in1
     ON (
      (in0.idf_pers_ods = in1.idf_pers_ods)
      AND (in0.numero_contrato_inversiones = in1.numero_contrato_inversiones)
    )

),

Formula_49_0 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    CAST(1 AS BOOLEAN) AS es_primer_titular,
    *
  
  FROM Join_48_inner AS in0

),

Join_48_left AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT in0.*
  
  FROM AlteryxSelect_44 AS in0
  ANTI JOIN Sample_39_drop_row_number_0 AS in1
     ON (
      (in0.idf_pers_ods = in1.idf_pers_ods)
      AND (in0.numero_contrato_inversiones = in1.numero_contrato_inversiones)
    )

),

Formula_50_0 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    CAST(0 AS BOOLEAN) AS es_primer_titular,
    *
  
  FROM Join_48_left AS in0

),

Union_51 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  {{
    prophecy_basics.UnionByName(
      ['Formula_49_0', 'Formula_50_0'], 
      [
        '[{"name": "es_primer_titular", "dataType": "Boolean"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "numero_contrato_inversiones", "dataType": "String"}, {"name": "fecha_dato", "dataType": "Date"}, {"name": "fecha_alta_titular", "dataType": "Date"}, {"name": "estado_persona", "dataType": "String"}, {"name": "fecha_inicio_w8", "dataType": "Date"}, {"name": "fecha_vencimiento_w8", "dataType": "Date"}, {"name": "estado_w8", "dataType": "String"}, {"name": "fecha_inicio_3103", "dataType": "Date"}, {"name": "fecha_vencimiento_3103", "dataType": "Date"}, {"name": "estado_3103", "dataType": "String"}, {"name": "fecha_inicio_306", "dataType": "Date"}, {"name": "fecha_vencimiento_306", "dataType": "Date"}, {"name": "estado_306", "dataType": "String"}, {"name": "DynCliTitARolId", "dataType": "String"}, {"name": "DynCliTitAPerTDoc", "dataType": "String"}, {"name": "DynCliTitAPerPDoc", "dataType": "String"}, {"name": "DynCliTitAPerDoc", "dataType": "String"}, {"name": "DynCliTitAPerNom1", "dataType": "String"}, {"name": "DynCliTitAPerNom2", "dataType": "String"}, {"name": "DynCliTitAPerApe1", "dataType": "String"}, {"name": "DynCliTitAPerApe2", "dataType": "String"}, {"name": "DynCliTitAFecMod", "dataType": "String"}, {"name": "DynCliTitAUsrAlt", "dataType": "String"}, {"name": "DynCliTitAUsrMod", "dataType": "String"}, {"name": "DynCliTitALoteDW", "dataType": "String"}, {"name": "DynCliTitAHash", "dataType": "String"}, {"name": "data_date_part", "dataType": "Integer"}, {"name": "data_timestamp_part", "dataType": "Timestamp"}]', 
        '[{"name": "es_primer_titular", "dataType": "Boolean"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "numero_contrato_inversiones", "dataType": "String"}, {"name": "fecha_dato", "dataType": "Date"}, {"name": "fecha_alta_titular", "dataType": "Date"}, {"name": "estado_persona", "dataType": "String"}, {"name": "fecha_inicio_w8", "dataType": "Date"}, {"name": "fecha_vencimiento_w8", "dataType": "Date"}, {"name": "estado_w8", "dataType": "String"}, {"name": "fecha_inicio_3103", "dataType": "Date"}, {"name": "fecha_vencimiento_3103", "dataType": "Date"}, {"name": "estado_3103", "dataType": "String"}, {"name": "fecha_inicio_306", "dataType": "Date"}, {"name": "fecha_vencimiento_306", "dataType": "Date"}, {"name": "estado_306", "dataType": "String"}, {"name": "DynCliTitARolId", "dataType": "String"}, {"name": "DynCliTitAPerTDoc", "dataType": "String"}, {"name": "DynCliTitAPerPDoc", "dataType": "String"}, {"name": "DynCliTitAPerDoc", "dataType": "String"}, {"name": "DynCliTitAPerNom1", "dataType": "String"}, {"name": "DynCliTitAPerNom2", "dataType": "String"}, {"name": "DynCliTitAPerApe1", "dataType": "String"}, {"name": "DynCliTitAPerApe2", "dataType": "String"}, {"name": "DynCliTitAFecMod", "dataType": "String"}, {"name": "DynCliTitAUsrAlt", "dataType": "String"}, {"name": "DynCliTitAUsrMod", "dataType": "String"}, {"name": "DynCliTitALoteDW", "dataType": "String"}, {"name": "DynCliTitAHash", "dataType": "String"}, {"name": "data_date_part", "dataType": "Integer"}, {"name": "data_timestamp_part", "dataType": "Timestamp"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_52 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    MAX(es_primer_titular) AS Max_es_primer_titular,
    numero_contrato_inversiones AS numero_contrato_inversiones
  
  FROM Union_51 AS in0
  
  GROUP BY numero_contrato_inversiones

),

Filter_53 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  {#Finds records where the main account holder is not the primary holder, which may help identify secondary account holders for targeted communication or compliance.#}
  SELECT * 
  
  FROM Summarize_52 AS in0
  
  WHERE (NOT CAST(Max_es_primer_titular AS BOOLEAN))

),

Join_54_inner AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`numero_contrato_inversiones`, `Max_es_primer_titular`)
  
  FROM Union_51 AS in0
  INNER JOIN Filter_53 AS in1
     ON (in0.numero_contrato_inversiones = in1.numero_contrato_inversiones)

),

AlteryxSelect_58 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT * EXCEPT (`es_primer_titular`)
  
  FROM Join_54_inner AS in0

),

MultiRowFormula_57_row_id_0 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    (monotonically_increasing_id()) AS prophecy_row_id,
    *
  
  FROM AlteryxSelect_58 AS in0

),

MultiRowFormula_57_0 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    (LAG(numero_contrato_inversiones, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id NULLS FIRST)) AS numero_contrato_inversiones_lag1,
    *
  
  FROM MultiRowFormula_57_row_id_0 AS in0

),

MultiRowFormula_57_1 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    CAST((
      CASE
        WHEN CAST((numero_contrato_inversiones = numero_contrato_inversiones_lag1) AS BOOLEAN)
          THEN 0
        ELSE 1
      END
    ) AS BOOLEAN) AS es_primer_titular,
    * EXCEPT (`numero_contrato_inversiones_lag1`)
  
  FROM MultiRowFormula_57_0 AS in0

),

Join_54_left AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT in0.*
  
  FROM Union_51 AS in0
  ANTI JOIN Filter_53 AS in1
     ON (in0.numero_contrato_inversiones = in1.numero_contrato_inversiones)

),

MultiRowFormula_57_row_id_drop_0 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_57_1 AS in0

),

Union_59 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  {{
    prophecy_basics.UnionByName(
      ['Join_54_left', 'MultiRowFormula_57_row_id_drop_0'], 
      [
        '[{"name": "es_primer_titular", "dataType": "Boolean"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "numero_contrato_inversiones", "dataType": "String"}, {"name": "fecha_dato", "dataType": "Date"}, {"name": "fecha_alta_titular", "dataType": "Date"}, {"name": "estado_persona", "dataType": "String"}, {"name": "fecha_inicio_w8", "dataType": "Date"}, {"name": "fecha_vencimiento_w8", "dataType": "Date"}, {"name": "estado_w8", "dataType": "String"}, {"name": "fecha_inicio_3103", "dataType": "Date"}, {"name": "fecha_vencimiento_3103", "dataType": "Date"}, {"name": "estado_3103", "dataType": "String"}, {"name": "fecha_inicio_306", "dataType": "Date"}, {"name": "fecha_vencimiento_306", "dataType": "Date"}, {"name": "estado_306", "dataType": "String"}, {"name": "DynCliTitARolId", "dataType": "String"}, {"name": "DynCliTitAPerTDoc", "dataType": "String"}, {"name": "DynCliTitAPerPDoc", "dataType": "String"}, {"name": "DynCliTitAPerDoc", "dataType": "String"}, {"name": "DynCliTitAPerNom1", "dataType": "String"}, {"name": "DynCliTitAPerNom2", "dataType": "String"}, {"name": "DynCliTitAPerApe1", "dataType": "String"}, {"name": "DynCliTitAPerApe2", "dataType": "String"}, {"name": "DynCliTitAFecMod", "dataType": "String"}, {"name": "DynCliTitAUsrAlt", "dataType": "String"}, {"name": "DynCliTitAUsrMod", "dataType": "String"}, {"name": "DynCliTitALoteDW", "dataType": "String"}, {"name": "DynCliTitAHash", "dataType": "String"}, {"name": "data_date_part", "dataType": "Integer"}, {"name": "data_timestamp_part", "dataType": "Timestamp"}]', 
        '[{"name": "es_primer_titular", "dataType": "Boolean"}, {"name": "idf_pers_ods", "dataType": "String"}, {"name": "numero_contrato_inversiones", "dataType": "String"}, {"name": "fecha_dato", "dataType": "Date"}, {"name": "fecha_alta_titular", "dataType": "Date"}, {"name": "estado_persona", "dataType": "String"}, {"name": "fecha_inicio_w8", "dataType": "Date"}, {"name": "fecha_vencimiento_w8", "dataType": "Date"}, {"name": "estado_w8", "dataType": "String"}, {"name": "fecha_inicio_3103", "dataType": "Date"}, {"name": "fecha_vencimiento_3103", "dataType": "Date"}, {"name": "estado_3103", "dataType": "String"}, {"name": "fecha_inicio_306", "dataType": "Date"}, {"name": "fecha_vencimiento_306", "dataType": "Date"}, {"name": "estado_306", "dataType": "String"}, {"name": "DynCliTitARolId", "dataType": "String"}, {"name": "DynCliTitAPerTDoc", "dataType": "String"}, {"name": "DynCliTitAPerPDoc", "dataType": "String"}, {"name": "DynCliTitAPerDoc", "dataType": "String"}, {"name": "DynCliTitAPerNom1", "dataType": "String"}, {"name": "DynCliTitAPerNom2", "dataType": "String"}, {"name": "DynCliTitAPerApe1", "dataType": "String"}, {"name": "DynCliTitAPerApe2", "dataType": "String"}, {"name": "DynCliTitAFecMod", "dataType": "String"}, {"name": "DynCliTitAUsrAlt", "dataType": "String"}, {"name": "DynCliTitAUsrMod", "dataType": "String"}, {"name": "DynCliTitALoteDW", "dataType": "String"}, {"name": "DynCliTitAHash", "dataType": "String"}, {"name": "data_date_part", "dataType": "Integer"}, {"name": "data_timestamp_part", "dataType": "Timestamp"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_220_0 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    CAST(regexp_replace(
      regexp_replace(format_number(CAST(((year(fecha_dato) * 100) + month(fecha_dato)) AS DOUBLE), 0), ',', '__THS__'), 
      '__THS__', 
      '') AS STRING) AS AAAAMM,
    *
  
  FROM Union_59 AS in0

),

Summarize_341 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT MAX(AAAAMM) AS Max_AAAAMM
  
  FROM Formula_220_0 AS in0

),

Join_342_inner AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`Max_AAAAMM`)
  
  FROM Formula_220_0 AS in0
  INNER JOIN Summarize_341 AS in1
     ON (in0.AAAAMM = in1.Max_AAAAMM)

),

AlteryxSelect_65 AS (

  {#VisualGroup: Tablapersonasparticipantesdeloscontratos#}
  SELECT 
    numero_contrato_inversiones AS numero_contrato_inversiones,
    idf_pers_ods AS idf_pers_ods,
    es_primer_titular AS es_primer_titular,
    fecha_dato AS fecha_dato,
    fecha_alta_titular AS fecha_alta_titular,
    CAST(estado_persona AS string) AS estado_persona,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(fecha_inicio_w8 AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(fecha_inicio_w8 AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(fecha_inicio_w8 AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(fecha_inicio_w8 AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(fecha_inicio_w8 AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_inicio_w8,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_w8 AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_w8 AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_w8 AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_w8 AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_w8 AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_vencimiento_w8,
    estado_w8 AS estado_w8,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(fecha_inicio_3103 AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(fecha_inicio_3103 AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(fecha_inicio_3103 AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(fecha_inicio_3103 AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(fecha_inicio_3103 AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_inicio_3103,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_3103 AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_3103 AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_3103 AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_3103 AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_3103 AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_vencimiento_3103,
    estado_3103 AS estado_3103,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(fecha_inicio_306 AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(fecha_inicio_306 AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(fecha_inicio_306 AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(fecha_inicio_306 AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(fecha_inicio_306 AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_inicio_306,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_306 AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_306 AS string), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_306 AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_306 AS string), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(fecha_vencimiento_306 AS string), 'yyyy-MM-dd'))
      END
    ) AS fecha_vencimiento_306,
    estado_306 AS estado_306,
    AAAAMM AS AAAAMM
  
  FROM Join_342_inner AS in0

)

SELECT *

FROM AlteryxSelect_65
