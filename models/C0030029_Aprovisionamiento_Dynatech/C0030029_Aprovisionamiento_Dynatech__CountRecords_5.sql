{{
  config({    
    "materialized": "ephemeral",
    "database": "akash_demos",
    "schema": "demos"
  })
}}

WITH AlteryxSelect_65 AS (

  SELECT *
  
  FROM {{ ref('C0030029_Aprovisionamiento_Dynatech__AlteryxSelect_65')}}

),

CountRecords_5 AS (

  {{ prophecy_basics.CountRecords(['AlteryxSelect_65'], [], 'count_all_records') }}

)

SELECT *

FROM CountRecords_5
