{{
  config({    
    "materialized": "ephemeral",
    "database": "dev_ref_control",
    "schema": "prophecy_tmp"
  })
}}

WITH dbo_dynclimot AS (

  SELECT * 
  
  FROM {{ source('dev_curated_uyasdbtest_dynvaldwsantander', 'dbo_dynclimot') }}

),

Filter_Data_Date_part_DynMoTFecVal AS (

  {#Filters records from Table_1 for entries after a dynamic date, set three months prior to a specified reference date.#}
  SELECT * 
  
  FROM dbo_dynclimot AS in0
  
  WHERE DynMoTFecVal > add_months(to_timestamp(CAST({{ var('ref_data_date_part') }} AS STRING), 'yyyyMMdd'), -3)

),

Filter_351 AS (

  SELECT * 
  
  FROM Filter_Data_Date_part_DynMoTFecVal AS in0
  
  WHERE (
          NOT(
            DynMoTEstSis = 'ANU')
        )

),

Formula_360_0 AS (

  {#Adjusts monetary values based on specific conditions for financial analysis.#}
  SELECT 
    CAST((
      CASE
        WHEN CAST((DynMoTMonEfe = 0) AS BOOLEAN)
          THEN DynMoTMonVlr
        ELSE DynMoTMonEfe
      END
    ) AS INTEGER) AS DynMoTMonEfe,
    * EXCEPT (`dynmotmonefe`)
  
  FROM Filter_351 AS in0

)

SELECT *

FROM Formula_360_0
