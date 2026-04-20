{{
  config({    
    "materialized": "table",
    "alias": "empty1",
    "database": "ayush_demos",
    "schema": "demos"
  })
}}

WITH Reformat_1 AS (

  SELECT *
  
  FROM {{ ref('p1__Reformat_1')}}

)

{#Loads data from the 'empty1' table in the 'ayush_demos_demos' source, overwriting previous data only if new data exists.#}
SELECT *

FROM Reformat_1
