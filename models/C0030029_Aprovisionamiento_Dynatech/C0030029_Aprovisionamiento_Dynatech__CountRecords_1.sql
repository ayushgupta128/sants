{{
  config({    
    "materialized": "ephemeral",
    "database": "akash_demos",
    "schema": "demos"
  })
}}

WITH Join_349_inner AS (

  SELECT *
  
  FROM {{ ref('C0030029_Aprovisionamiento_Dynatech__Join_349_inner')}}

),

CountRecords_1 AS (

  {#VisualGroup: HistoricoMovimientos#}
  {{ prophecy_basics.CountRecords(['Join_349_inner'], [], 'count_all_records') }}

)

SELECT *

FROM CountRecords_1
