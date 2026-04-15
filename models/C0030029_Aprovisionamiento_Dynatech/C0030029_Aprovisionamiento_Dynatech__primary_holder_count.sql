{{
  config({    
    "materialized": "ephemeral",
    "database": "dev_ref_control",
    "schema": "prophecy_tmp"
  })
}}

WITH AlteryxSelect_65 AS (

  SELECT *
  
  FROM {{ ref('C0030029_Aprovisionamiento_Dynatech__AlteryxSelect_65')}}

),

primary_holder_count AS (

  {#Counts occurrences of primary account holders.#}
  SELECT 
    es_primer_titular AS es_primer_titular,
    count(es_primer_titular) AS count
  
  FROM AlteryxSelect_65 AS in0
  
  GROUP BY es_primer_titular

)

SELECT *

FROM primary_holder_count
