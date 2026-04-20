{{
  config({    
    "materialized": "ephemeral",
    "database": "ayush_demos",
    "schema": "demos"
  })
}}

WITH initial_table AS (

  SELECT * 
  
  FROM {{ source('ayush_demos_demos', 'initial_table') }}

),

Reformat_1 AS (

  SELECT * 
  
  FROM initial_table AS in0

)

SELECT *

FROM Reformat_1
