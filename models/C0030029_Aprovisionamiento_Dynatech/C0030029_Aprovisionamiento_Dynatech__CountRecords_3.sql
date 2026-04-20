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

primary_holder_records AS (

  {#Identifies records of primary account holders.#}
  SELECT * 
  
  FROM AlteryxSelect_65 AS in0
  
  WHERE es_primer_titular = true

),

CountRecords_3 AS (

  {{ prophecy_basics.CountRecords(['primary_holder_records'], [], 'count_all_records') }}

)

SELECT *

FROM CountRecords_3
