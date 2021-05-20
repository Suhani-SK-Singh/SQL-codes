CREATE OR REPLACE PROCEDURE donedeal_aggregate.sp_fact_zoura_ad_analytics_monthly()
	LANGUAGE plpgsql
AS $$
	
	
	
	


	DECLARE
	v_etl_log_id BIGINT DEFAULT 0;
	v_platform_name VARCHAR = 'DONEDEAL';
	v_task_name VARCHAR = 'sp_fact_zoura_ad_analytics_monthly';
	v_target_schema VARCHAR = 'DONEDEAL_AGGREGATE';
	v_target_table VARCHAR = 'FACT_ZOURA_AD_ANALYTICS_MONTHLY';
	v_start_ts TIMESTAMP = CURRENT_TIMESTAMP;
	v_end_ts TIMESTAMP DEFAULT NULL;
	v_status VARCHAR DEFAULT 'FAILURE';
	v_num_recs BIGINT DEFAULT 0;
	v_err_msg VARCHAR DEFAULT 'NA';
	
	BEGIN

	-- **********************
	-- Create ETL Log Record
	-- **********************
	CALL core_prod.sp_create_etl_log
		(v_platform_name
		,v_task_name
		,v_target_schema
		,v_target_table
		,v_start_ts
		,v_end_ts
		,v_status
		,v_num_recs
		,v_err_msg
		,v_etl_log_id);


    -- ********
	-- DDL
	-- ********
	
	DROP TABLE IF EXISTS donedeal_aggregate.fact_zoura_ad_analytics_monthly;
	CREATE TABLE IF NOT EXISTS donedeal_aggregate.fact_zoura_ad_analytics_monthly
(   
     fact_zoura_ad_id BIGINT IDENTITY(1,1) NOT NULL ENCODE az64,
     dim_company_id  INT ENCODE az64,
	 year_month INT ENCODE az64,
	 formatted_main_category VARCHAR(2400) ENCODE lzo,
	 ad_listing_package VARCHAR(50) ENCODE lzo,
	 Total_Ad_Views INT ENCODE az64,
	 Total_Phone_reveals INT ENCODE az64,
	 Total_Calls INT ENCODE az64,
	 Total_Conversations INT ENCODE az64,
	 Total_SMS INT ENCODE az64,
	 Total_Spots INT ENCODE az64,
	 Total_Bumps INT ENCODE az64,
	 Total_Unique_Ads_Count INT ENCODE az64,
	 total_ads_in_price_bucket_0_to_5k INT ENCODE az64,
	 total_ads_in_price_bucket_5k_to_10k INT ENCODE az64,
	 total_ads_in_price_bucket_10k_to_15k INT ENCODE az64,
	 total_ads_in_price_bucket_15k_to_20k INT ENCODE az64,
	 total_ads_in_price_bucket_20k_to_25k INT ENCODE az64,
	 total_ads_in_price_bucket_25k_to_30k INT ENCODE az64,
	 total_ads_in_price_bucket_30k_to_40k INT ENCODE az64,
	 total_ads_in_price_bucket_40k_to_50k INT ENCODE az64,
	 total_ads_in_price_bucket_50k_to_60k INT ENCODE az64,
	 total_ads_in_price_bucket_60k_to_70k INT ENCODE az64,
	 total_ads_in_price_bucket_70k_to_80k INT ENCODE az64,
	 total_ads_in_price_bucket_80k_to_90k INT ENCODE az64,
	 total_ads_in_price_bucket_90k_to_100 INT ENCODE az64,
	 total_ads_in_price_bucket_greater_than_100k INT ENCODE az64,
	 Avg_number_of_ads INT ENCODE az64,
	 max_number_of_ads INT ENCODE az64,
	 min_number_of_ads INT ENCODE az64,
	 salesforce_account_id VARCHAR(50) ENCODE lzo,
     sales_rep_name VARCHAR(50) ENCODE lzo,
     dealer_classification VARCHAR(100) ENCODE lzo,
     bad_debt_charge_amt DOUBLE PRECISION   ENCODE RAW,
     credit_charge_amt DOUBLE PRECISION   ENCODE RAW,
     discount_charge_amt DOUBLE PRECISION   ENCODE RAW,
     overage_charge_amt DOUBLE PRECISION   ENCODE RAW,
     programme_upsell_charge_amt DOUBLE PRECISION   ENCODE RAW,
     programme_charge_amt DOUBLE PRECISION   ENCODE RAW,
     other_charge_amt DOUBLE PRECISION   ENCODE RAW ,
	 PRIMARY KEY (fact_zoura_ad_id)
	 )
	 DISTSTYLE KEY
 DISTKEY (dim_company_id)
 SORTKEY (
	dim_company_id
	);
	 
	---ALTER TABLE donedeal_aggregate.fact_zoura_ad_analytics_monthly owner to dddw_admin; 
	 --********
	-- DML
	-- ********

 INSERT INTO donedeal_aggregate.fact_zoura_ad_analytics_monthly(
                  dim_company_id ,
	              year_month ,
	              formatted_main_category ,
	              ad_listing_package ,
	              total_ad_views ,
	              total_phone_reveals ,
	              total_calls ,
	              total_conversations ,
	              total_sms,
	              total_spots ,
	              total_bumps ,
	              total_unique_ads_count ,
	              total_ads_in_price_bucket_0_to_5k,
	              total_ads_in_price_bucket_5k_to_10k ,
	              total_ads_in_price_bucket_10k_to_15k ,
	              total_ads_in_price_bucket_15k_to_20k ,
	              total_ads_in_price_bucket_20k_to_25k ,
	              total_ads_in_price_bucket_25k_to_30k ,
	              total_ads_in_price_bucket_30k_to_40k ,
	              total_ads_in_price_bucket_40k_to_50k ,
	              total_ads_in_price_bucket_50k_to_60k ,
	              total_ads_in_price_bucket_60k_to_70k ,
	              total_ads_in_price_bucket_70k_to_80k ,
	              total_ads_in_price_bucket_80k_to_90k ,
	              total_ads_in_price_bucket_90k_to_100 ,
	              total_ads_in_price_bucket_greater_than_100k ,
	              avg_number_of_ads ,
	              max_number_of_ads ,
	              min_number_of_ads ,
	              salesforce_account_id,
                  sales_rep_name,
                  dealer_classification ,
                  bad_debt_charge_amt ,
                  credit_charge_amt ,
                  discount_charge_amt ,
                  overage_charge_amt ,
                  programme_upsell_charge_amt ,
                  programme_charge_amt ,
                  other_charge_amt  )
WITH agg1 AS
(
	SELECT 
		dc.dim_company_id ,
		fdaa.nk_ad_id ,
		dc.ad_listing_package ,
		dd.year_month,
		CASE
			WHEN fdaa.main_category = 'Cars & Motor' THEN 'Cars & Motor'
			WHEN fdaa.main_category = 'Farming' THEN 'Farming'
			ELSE 'Other'
		END AS formatted_main_category,
		fdaa.ad_views_total,
		fdaa.phone_reveals_total,
		fdaa.calls_total,
		fdaa.conversations_total,
		fdaa.sms_total,
		fdaa.spots_total,
		fdaa.bumps_total,
		CONCAT( dc.dim_company_id, dd.year_month ) AS unique_key_agg1
	FROM donedeal_aggregate.fact_daily_ad_analytics fdaa
	INNER JOIN donedeal_aggregate.dim_company dc ON dc.nk_company_id = fdaa.nk_company_id 
	INNER JOIN core_prod.dim_date dd ON dd.calendar_date = fdaa.event_date
	WHERE fdaa.publish_dt > '2019-01-01' AND dc.nk_company_id > 0
	GROUP BY 
		dc.dim_company_id, fdaa.nk_ad_id, formatted_main_category, dc.ad_listing_package, dd.year_month, 
		fdaa.ad_views_total, fdaa.phone_reveals_total, fdaa.calls_total, fdaa.conversations_total, fdaa.sms_total,
		fdaa.spots_total, fdaa.bumps_total, unique_key_agg1
	ORDER BY dc.dim_company_id, dd.year_month
),
agg2 AS 
(
	WITH agg2iq1 AS (
		SELECT
		   dc.dim_company_id,
		   dd.year_month ,
		   COUNT( DISTINCT( nk_ad_id )) AS count_ad
		FROM donedeal_aggregate.fact_daily_ad_analytics fdaa
		INNER JOIN donedeal_aggregate.dim_company dc ON dc.nk_company_id = fdaa.nk_company_id
		LEFT JOIN core_prod.dim_date dd ON dd.calendar_date = fdaa.event_date
		WHERE publish_dt > '2019-01-01' and  dc.nk_company_id >0
		GROUP BY 
			dc.dim_company_id, dd.year_month, dd.day_of_month, dd.calendar_date 
		ORDER BY 
			dc.dim_company_id, dd.year_month, dd.calendar_date 
	),
	agg2iq2 AS (
		SELECT
			MAX(day_of_month) AS Total_Days_In_Month,
			year_month 
		FROM 
		core_prod.dim_date dd2 
		GROUP BY year_month 
		ORDER BY year_month 
	)
	SELECT 
		agg2iq1.dim_company_id,
		agg2iq1.year_month,
		agg2iq2.Total_Days_In_Month,
		CEILING (ROUND( SUM(count_ad) * 1.0 / agg2iq2.Total_Days_In_Month, 2)) AS Avg_number_of_ads,
		MAX(count_ad) AS max_number_of_ads,
		MIN(count_ad) AS min_number_of_ads,
		CONCAT( agg2iq1.dim_company_id, agg2iq1.year_month ) AS unique_key_agg2
	FROM agg2iq1
	LEFT JOIN agg2iq2 ON agg2iq2.year_month = agg2iq1.year_month
	GROUP BY 
		agg2iq1.dim_company_id, agg2iq1.year_month, agg2iq2.Total_Days_In_Month, unique_key_agg2
	ORDER BY 
		agg2iq1.dim_company_id, agg2iq1.year_month
),
agg3 AS 
(
	WITH agg3iq1 AS 
	(
		SELECT 
			dc.dim_company_id,
			fdaa.nk_ad_id,
			dd.year_month,
			dd.calendar_date,
			fdaa.latest_price,
			CASE WHEN latest_price > 0 and latest_price < 5000 THEN 1 ELSE 0 END AS price_bucket_0_to_5k,
			CASE WHEN latest_price >5000 and latest_price <10000 THEN 1 ELSE 0 END AS price_bucket_5k_to_10k,
			CASE WHEN latest_price >10000 and latest_price <15000 THEN 1 ELSE 0 END AS price_bucket_10k_to_15k,
			CASE WHEN latest_price >15000 and latest_price <20000 THEN 1 ELSE 0 END AS price_bucket_15k_to_20k,
			CASE WHEN latest_price >20000 and latest_price <25000 THEN 1 ELSE 0 END AS price_bucket_20k_to_25k,
			CASE WHEN latest_price >25000 and latest_price <30000 THEN 1 ELSE 0 END AS price_bucket_25k_to_30k,
			CASE WHEN latest_price >30000 and latest_price <40000 THEN 1 ELSE 0 END AS price_bucket_30k_to_40k,
			CASE WHEN latest_price >40000 and latest_price <50000 THEN 1 ELSE 0 END AS price_bucket_40k_to_50k,
			CASE WHEN latest_price >50000 and latest_price <60000 THEN 1 ELSE 0 END AS price_bucket_50k_to_60k,
			CASE WHEN latest_price >60000 and latest_price <70000 THEN 1 ELSE 0 END AS price_bucket_60k_to_70k,
			CASE WHEN latest_price >70000 and latest_price <80000 THEN 1 ELSE 0 END AS price_bucket_70k_to_80k,
			CASE WHEN latest_price >80000 and latest_price <90000 THEN 1 ELSE 0 END AS price_bucket_80k_to_90k,
			CASE WHEN latest_price >90000 and latest_price <100000 THEN 1 ELSE 0 END AS price_bucket_90k_to_100k,
			CASE WHEN latest_price >100000  THEN 1 ELSE 0 END AS price_bucket_greater_than_100k,
			ROW_NUMBER() OVER ( PARTITION BY dc.dim_company_id,fdaa.nk_ad_id, dd.year_month ORDER BY dd.calendar_date DESC ) AS rn,
			CONCAT( dc.dim_company_id, dd.year_month ) AS unique_key_agg3
		FROM donedeal_aggregate.fact_daily_ad_analytics fdaa
		INNER JOIN 
			donedeal_aggregate.dim_company dc ON dc.nk_company_id = fdaa.nk_company_id 
		INNER JOIN 
			core_prod.dim_date dd ON dd.calendar_date = fdaa.event_date
		WHERE 
			fdaa.publish_dt > '2019-01-01' AND dc.nk_company_id > 0
		GROUP BY 
			dc.dim_company_id, fdaa.nk_ad_id, dd.year_month, 
			dd.calendar_date, fdaa.latest_price, unique_key_agg3 
		ORDER BY 
			dc.dim_company_id, fdaa.nk_ad_id, dd.year_month, 
			dd.calendar_date DESC, fdaa.latest_price
	),
	agg3iq2 AS 
	(
		SELECT
			*
		FROM agg3iq1
		WHERE rn = 1
	)
	SELECT
		agg3iq2.dim_company_id,
		agg3iq2.year_month,
		SUM( agg3iq2.price_bucket_0_to_5k ) AS total_ads_in_price_bucket_0_to_5k,
		SUM( agg3iq2.price_bucket_5k_to_10k ) AS total_ads_in_price_bucket_5k_to_10k,
		SUM( agg3iq2.price_bucket_10k_to_15k ) AS total_ads_in_price_bucket_10k_to_15k,
		SUM( agg3iq2.price_bucket_15k_to_20k ) AS total_ads_in_price_bucket_15k_to_20k,
		SUM( agg3iq2.price_bucket_20k_to_25k ) AS total_ads_in_price_bucket_20k_to_25k,
		SUM( agg3iq2.price_bucket_25k_to_30k ) AS total_ads_in_price_bucket_25k_to_30k,
		SUM( agg3iq2.price_bucket_30k_to_40k ) AS total_ads_in_price_bucket_30k_to_40k,
		SUM( agg3iq2.price_bucket_40k_to_50k ) AS total_ads_in_price_bucket_40k_to_50k,
		SUM( agg3iq2.price_bucket_50k_to_60k ) AS total_ads_in_price_bucket_50k_to_60k,
		SUM( agg3iq2.price_bucket_60k_to_70k ) AS total_ads_in_price_bucket_60k_to_70k,
		SUM( agg3iq2.price_bucket_70k_to_80k ) AS total_ads_in_price_bucket_70k_to_80k,
		SUM( agg3iq2.price_bucket_80k_to_90k ) AS total_ads_in_price_bucket_80k_to_90k,
		SUM( agg3iq2.price_bucket_90k_to_100k ) AS total_ads_in_price_bucket_90k_to_100,
		SUM( agg3iq2.price_bucket_greater_than_100k ) AS total_ads_in_price_bucket_greater_than_100k,
		agg3iq2.unique_key_agg3
	FROM agg3iq2
	GROUP BY 
		agg3iq2.dim_company_id,
		agg3iq2.year_month,
		agg3iq2.unique_key_agg3
	ORDER BY
		agg3iq2.dim_company_id,
		agg3iq2.year_month
), 
agg4 AS (
       WITH agg as
         (
             SELECT 
                    dza.dim_company_id,
                    dd.year_month,
                    dza.salesforce_account_id,
                    dza.sales_rep_name,
                    dza.dealer_classification ,
                    CASE WHEN zch.report_category = 'Bad Debt' THEN sum(dzii.charge_amt) END as bad_debt_charge_amt,
                    CASE WHEN zch.report_category = 'Credit' THEN sum(dzii.charge_amt) END as credit_charge_amt,
                    CASE WHEN zch.report_category = 'Discount' THEN sum(dzii.charge_amt) END as discount_charge_amt,
                    CASE WHEN zch.report_category = 'Overage' THEN sum(dzii.charge_amt) END as overage_charge_amt,
                    CASE WHEN zch.report_category = 'Programme Upsell' THEN sum(dzii.charge_amt) END as programme_upsell_charge_amt,
                    CASE WHEN zch.report_category = 'Programme' THEN sum(dzii.charge_amt) END as programme_charge_amt,
                    CASE WHEN zch.report_category NOT IN ('Bad Debt', 'Programme', 'Credit', 'Discount', 'Overage', 'Programme Upsell', 'Programme') THEN sum(dzii.charge_amt) END as other_charge_amt
             FROM donedeal_aggregate.dim_zoura_account dza
            INNER JOIN donedeal_aggregate.dim_company dcom on dcom.dim_company_id = dza.dim_company_id AND dcom.nk_company_id > 0
            INNER JOIN donedeal_aggregate.dim_zoura_invoice dzi on dzi.dim_zoura_account_id = dza.dim_zoura_account_id
            INNER JOIN core_prod.dim_date dd on dd.calendar_date = dzi.invoice_dt::DATE
            INNER JOIN donedeal_aggregate.dim_zoura_invoice_item dzii on dzii.dim_zoura_invoice_id = dzi.dim_zoura_invoice_id
            INNER JOIN donedeal_aggregate.dim_zoura_charge_hierarchy zch on zch.dim_zoura_charge_hierarchy_id = dzii.dim_zoura_charge_hierarchy_id
            WHERE dzi.invoice_dt > '2019-01-01'
              AND UPPER(dzi.invoice_status) = 'POSTED' 
             GROUP BY dza.dim_company_id,
                    dd.year_month,
                    dza.salesforce_account_id,
                    dza.sales_rep_name,
                    dza.dealer_classification ,
                    zch.report_category
             ORDER BY dza.dim_company_id,
                    dd.year_month
         )
SELECT
       MAX(a.dim_company_id) AS dim_company_id,
       a.year_month,
       MAX(a.salesforce_account_id) As salesforce_account_id,
       MAX(a.sales_rep_name) AS sales_rep_name,
       MAX(a.dealer_classification) AS dealer_classification,
       SUM(NVL(a.bad_debt_charge_amt,0)) AS bad_debt_charge_amt,
       SUM(NVL(a.credit_charge_amt,0)) AS credit_charge_amt,
       SUM(NVL(a.discount_charge_amt,0)) AS discount_charge_amt,
       SUM(NVL(a.overage_charge_amt,0)) AS overage_charge_amt,
       SUM(NVL(a.programme_upsell_charge_amt,0)) AS programme_upsell_charge_amt,
       SUM(NVL(a.programme_charge_amt,0)) AS programme_charge_amt,
       SUM(NVL(a.other_charge_amt,0)) AS other_charge_amt,
       CONCAT( a.dim_company_id, a.year_month ) AS unique_key_agg4
FROM agg a
GROUP BY a.dim_company_id, a.year_month
ORDER BY a.dim_company_id, a.year_month)
SELECT 
	a1.dim_company_id,
	a1.year_month,
	MAX( a1.formatted_main_category ) AS formatted_main_category,
	MAX( a1.ad_listing_package ) AS ad_listing_package,
	SUM( ad_views_total ) AS Total_Ad_Views,
	SUM( phone_reveals_total ) AS Total_Phone_reveals,
	SUM( calls_total ) AS Total_Calls,
	SUM( conversations_total ) AS Total_Conversations,
	SUM( sms_total) AS Total_SMS,
	SUM( spots_total ) AS Total_Spots,
	SUM( bumps_total ) AS Total_Bumps,
	COUNT( DISTINCT( a1.nk_ad_id )) AS Total_Unique_Ads_Count,
	a3.total_ads_in_price_bucket_0_to_5k,
	a3.total_ads_in_price_bucket_5k_to_10k,
	a3.total_ads_in_price_bucket_10k_to_15k,
	a3.total_ads_in_price_bucket_15k_to_20k,
	a3.total_ads_in_price_bucket_20k_to_25k,
	a3.total_ads_in_price_bucket_25k_to_30k,
	a3.total_ads_in_price_bucket_30k_to_40k,
	a3.total_ads_in_price_bucket_40k_to_50k,
	a3.total_ads_in_price_bucket_50k_to_60k,
	a3.total_ads_in_price_bucket_60k_to_70k,
	a3.total_ads_in_price_bucket_70k_to_80k,
	a3.total_ads_in_price_bucket_80k_to_90k,
	a3.total_ads_in_price_bucket_90k_to_100,
	a3.total_ads_in_price_bucket_greater_than_100k,
	a2.Avg_number_of_ads,
	a2.max_number_of_ads,
	a2.min_number_of_ads,
	a4.salesforce_account_id,
    a4.sales_rep_name,
    a4.dealer_classification,
    a4.bad_debt_charge_amt,
    a4.credit_charge_amt,
    a4.discount_charge_amt,
    a4.overage_charge_amt,
    a4.programme_upsell_charge_amt,
    a4.programme_charge_amt,
    a4.other_charge_amt
FROM agg1 a1
INNER JOIN agg2 a2 ON a2.unique_key_agg2 = a1.unique_key_agg1
INNER JOIN agg3 a3 ON a3.unique_key_agg3 = a1.unique_key_agg1
INNER JOIN agg4 a4 ON a4.unique_key_agg4 = a1.unique_key_agg1
GROUP BY 
	a1.dim_company_id, a1.year_month,
	a3.total_ads_in_price_bucket_0_to_5k,
	a3.total_ads_in_price_bucket_5k_to_10k,
	a3.total_ads_in_price_bucket_10k_to_15k,
	a3.total_ads_in_price_bucket_15k_to_20k,
	a3.total_ads_in_price_bucket_20k_to_25k,
	a3.total_ads_in_price_bucket_25k_to_30k,
	a3.total_ads_in_price_bucket_30k_to_40k,
	a3.total_ads_in_price_bucket_40k_to_50k,
	a3.total_ads_in_price_bucket_50k_to_60k,
	a3.total_ads_in_price_bucket_60k_to_70k,
	a3.total_ads_in_price_bucket_70k_to_80k,
	a3.total_ads_in_price_bucket_80k_to_90k,
	a3.total_ads_in_price_bucket_90k_to_100,
	a3.total_ads_in_price_bucket_greater_than_100k,
	a2.Avg_number_of_ads, a2.max_number_of_ads, a2.min_number_of_ads,
	a4.salesforce_account_id,
    a4.sales_rep_name,
    a4.dealer_classification,
    a4.bad_debt_charge_amt,
    a4.credit_charge_amt,
    a4.discount_charge_amt,
    a4.overage_charge_amt,
    a4.programme_upsell_charge_amt,
    a4.programme_charge_amt,
    a4.other_charge_amt
ORDER BY 
	a1.dim_company_id, a1.year_month;
	
	
	  -- **************************
	  -- Analyze Table post insert
	  -- **************************

    ANALYZE donedeal_aggregate.fact_zoura_ad_analytics_monthly;



-- Update ETL with job completion status

v_status = 'SUCCESS';
v_end_ts = CURRENT_TIMESTAMP;

CALL core_prod.sp_update_etl_log
	(v_etl_log_id
	,v_num_recs
	,v_status
	,v_err_msg
	,v_end_ts
	);

EXCEPTION
  WHEN OTHERS THEN

    RAISE INFO 'error message SQLERRM %', SQLERRM;
    RAISE INFO 'error message SQLSTATE %', SQLSTATE;

END;















$$
;
