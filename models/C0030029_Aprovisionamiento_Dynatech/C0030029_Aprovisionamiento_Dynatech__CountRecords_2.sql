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

CountRecords_2 AS (

  {#VisualGroup: HistoricoMovimientos#}
  {{ prophecy_basics.CountRecords(['Formula_360_0'], [], 'count_all_records') }}

)

SELECT *

FROM CountRecords_2
