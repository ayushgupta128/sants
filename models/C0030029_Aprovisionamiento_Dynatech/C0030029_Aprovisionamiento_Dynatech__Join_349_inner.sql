{{
  config({    
    "materialized": "ephemeral",
    "database": "akash_demos",
    "schema": "demos"
  })
}}

WITH Formula_360_0 AS (

  SELECT *
  
  FROM {{ ref('C0030029_Aprovisionamiento_Dynatech__Formula_360_0')}}

),

dbo_dyncodigene AS (

  {#VisualGroup: HistoricoMovimientos#}
  {#Overwrites the dataset for curated gene data in the development database.#}
  SELECT * 
  
  FROM {{ source('dev_curated_uyasdbtest_dynvaldwsantander', 'dbo_dyncodigene') }}

),

FilterDataDatePartCodiGene AS (

  {#VisualGroup: HistoricoMovimientos#}
  {#Filters records based on a specific date part for targeted analysis.#}
  SELECT * 
  
  FROM dbo_dyncodigene AS in0
  
  WHERE data_date_part == {{ var('ref_data_date_part') }}

),

AlteryxSelect_350 AS (

  {#VisualGroup: HistoricoMovimientos#}
  SELECT 
    CAST(DynCoGCodi AS INTEGER) AS DynCoGCodi,
    DynCoGSmb AS moneda_movimiento
  
  FROM FilterDataDatePartCodiGene AS in0

),

Join_349_inner AS (

  {#VisualGroup: HistoricoMovimientos#}
  SELECT 
    in0.DynMoTFecVal AS DynMoTFecVal,
    in0.DynMoTFecOpe AS DynMoTFecOpe,
    in0.* EXCEPT (`DynMoTFecVal`, `DynMoTFecOpe`),
    in1.* EXCEPT (`DynCoGCodi`)
  
  FROM Formula_360_0 AS in0
  INNER JOIN AlteryxSelect_350 AS in1
     ON (in0.DynMoTMonEfe = in1.DynCoGCodi)

)

SELECT *

FROM Join_349_inner
