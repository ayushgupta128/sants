{{
  config({    
    "materialized": "ephemeral",
    "database": "dev_ref_control",
    "schema": "prophecy_tmp"
  })
}}

WITH Join_349_inner AS (

  SELECT *
  
  FROM {{ ref('C0030029_Aprovisionamiento_Dynatech__Join_349_inner')}}

),

CountRecords_1 AS (

  {{ prophecy_basics.CountRecords(['Join_349_inner'], [], 'count_all_records') }}

)

SELECT *

FROM CountRecords_1
