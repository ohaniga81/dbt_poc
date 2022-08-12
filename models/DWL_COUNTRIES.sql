{{
    config(
        tags=['NDW'],
		materialized="incremental",
        unique_key='cntry_code',
		incremental_strategy='merge',
		merge_update_columns = ['CNTRY_NAME', 'DELETED_FLAG', 'UPDATED_DT', 'UPDATED_PROCESS_ID', 'UPDATED_EFFECT_DATE', 'CNTRY_ALPHA3_CODE', 'CNTRY_NAME_RO']
		)
}}

WITH 
using_clause AS (
select 
'ADDR' source_system_code, 
a.cod, 
a.alpha2, 
a.alpha3, 
a.name_en, 
a.name_ro
from {{ source('STG', 'STG_ADDR_COUNTRIES') }} a where file_date='{{ var("p_date") }}'
),

updates AS (
        SELECT
        source_system_code	 						AS SOURCE_SYSTEM_CODE, 
		alpha2 						                AS CNTRY_CODE, 
		name_en    			                        AS CNTRY_NAME,
		'N' 										AS DELETED_FLAG,
		DATETIME_TRUNC(CURRENT_DATETIME(), SECOND) AS CREATED_DT,
		DATETIME_TRUNC(CURRENT_DATETIME(), SECOND) AS UPDATED_DT,
        999 										AS CREATED_PROCESS_ID, 
		123 										AS UPDATED_PROCESS_ID,  
		cast('2022-01-01' as date) 					AS CREATED_EFFECT_DATE, 
		cast('{{ var("p_date") }}' as date) 		AS UPDATED_EFFECT_DATE,
        alpha3									    AS CNTRY_ALPHA3_CODE, 
		name_ro 							        AS CNTRY_NAME_RO
    FROM using_clause
		 {% if is_incremental() %}
		WHERE  alpha2 IN (SELECT cntry_code FROM {{ this }})
		{% endif %}
),

inserts AS (
    SELECT
        source_system_code	 						AS SOURCE_SYSTEM_CODE, 
		alpha2 										AS CNTRY_CODE, 
		name_en    			                        AS CNTRY_NAME,
		'N' 										AS DELETED_FLAG,
		DATETIME_TRUNC(CURRENT_DATETIME(), SECOND) AS CREATED_DT,
		DATETIME_TRUNC(CURRENT_DATETIME(), SECOND) AS UPDATED_DT,
        999 										AS CREATED_PROCESS_ID, 
		456 										AS UPDATED_PROCESS_ID,  
		cast('2022-01-01' as date) 					AS CREATED_EFFECT_DATE, 
		cast('{{ var("p_date") }}' as date) 		AS UPDATED_EFFECT_DATE,
        alpha3									    AS CNTRY_ALPHA3_CODE, 
		name_ro 							        AS CNTRY_NAME_RO
    FROM using_clause
    WHERE 
	cast(alpha2 as string)  NOT IN (SELECT cast(cntry_code as string) FROM updates)
),

deletes AS (
        SELECT
        source_system_code	 						AS SOURCE_SYSTEM_CODE, 
		CNTRY_CODE									AS CNTRY_CODE, 
		CNTRY_NAME    			                    AS CNTRY_NAME,
		'Y' 										AS DELETED_FLAG,
		DATETIME_TRUNC(CURRENT_DATETIME(), SECOND) AS CREATED_DT,
		DATETIME_TRUNC(CURRENT_DATETIME(), SECOND) AS UPDATED_DT,
        999 										AS CREATED_PROCESS_ID, 
		789 										AS UPDATED_PROCESS_ID,  
		cast('2022-01-01' as date) 					AS CREATED_EFFECT_DATE, 
		cast('{{ var("p_date") }}' as date) 		AS UPDATED_EFFECT_DATE,
        CNTRY_ALPHA3_CODE							AS CNTRY_ALPHA3_CODE, 
		CNTRY_NAME_RO 							    AS CNTRY_NAME_RO
    FROM {{ this }}
		 {% if is_incremental() %}
		WHERE  cntry_code NOT IN (SELECT alpha2 FROM using_clause)
		and DELETED_FLAG='N'
		{% endif %}
)

SELECT * FROM inserts 
UNION ALL
SELECT * FROM updates
UNION ALL
SELECT * FROM deletes

