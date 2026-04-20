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

non_primary_account_holders AS (

  {#Identifies account holders who are not primary.#}
  SELECT * 
  
  FROM AlteryxSelect_65 AS in0
  
  WHERE es_primer_titular = false

),

CountRecords_4 AS (

  {#Counts all records of non-primary account holders for business insights.#}
  {{ prophecy_basics.CountRecords(['non_primary_account_holders'], [], 'count_all_records') }}

)

SELECT *

FROM CountRecords_4
