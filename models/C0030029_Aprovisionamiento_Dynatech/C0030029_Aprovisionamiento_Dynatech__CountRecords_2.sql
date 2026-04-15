{{
  config({    
    "materialized": "ephemeral",
    "database": "dev_ref_control",
    "schema": "prophecy_tmp"
  })
}}

WITH Formula_360_0 AS (

  SELECT *
  
  FROM {{ ref('C0030029_Aprovisionamiento_Dynatech__Formula_360_0')}}

),

CountRecords_2 AS (

  {{ prophecy_basics.CountRecords(['Formula_360_0'], [], 'count_all_records') }}

)

SELECT *

FROM CountRecords_2
