 SELECT u.id,
        MAX(publish_ts) AS last_publish_ts,
        MAX(bump_dt) AS last_bump_date,
        CASE WHEN v.email IS NOT NULL THEN 'Y' ELSE 'N' END AS email_verified_flag,
        CASE WHEN v.phone IS NOT NULL THEN 'Y' ELSE 'N' END AS phone_verified_flag,
        DATE(v.email) AS email_verification_date,
        DATE(v.phone) AS phone_verification_date,
        CASE WHEN u.status ='BLOCKED' THEN 'Y' ELSE 'N' END AS user_blocked_flag,
        u.lastlogindate AS last_login_date
 FROM donedeal_prod.users u
 LEFT JOIN donedeal_prod.verification v ON v.userid = u.id
 LEFT JOIN donedeal_aggregate.fact_ad fa ON fa.nk_user_id =u.id
 ----WHERE u.id =43202
 GROUP BY u.id,v.email,v.phone,u.status,u.lastlogindate 
 ORDER BY u.id
 
 
 
 ------2nd part
 
WITH U1 AS (
              SELECT uel.userid ,uel.ts,s.name,
              ROW_NUMBER() OVER ( PARTITION BY uel.userid  ORDER BY uel.ts DESC ) AS rn,
              COUNT(*) OVER ( PARTITION BY uel.userid ) as logins_total_num
              FROM donedeal_prod.user_event_log uel 
              INNER JOIN donedeal_prod.user_event ue ON ue.id = uel.event
              INNER JOIN donedeal_prod.source s ON s.id = uel.source
              WHERE ue.name IN('Login') AND userid IN ('43202','3274208')
              GROUP BY userid,uel.ts,s.name
              ORDER BY uel.userid)
SELECT U1.userid ,U1.logins_total_num,
       U1.name AS last_login_platform_name,
       DATE(u1.ts) as last_login_date  
from U1 where rn =1; ---2494987


--------finali 1--



WITH U1 AS (
              SELECT 
              uel.userid ,
              uel.ts,s.name,
              ROW_NUMBER() OVER ( PARTITION BY uel.userid  ORDER BY uel.ts DESC ) AS rn,
              COUNT(*) OVER ( PARTITION BY uel.userid ) as logins_total_num
              FROM donedeal_prod.user_event_log uel 
              INNER JOIN donedeal_prod.user_event ue ON ue.id = uel.event
              INNER JOIN donedeal_prod.source s ON s.id = uel.source
              WHERE ue.name IN('Login') -----AND userid IN ('43202','3274208','1')
              GROUP BY userid,uel.ts,s.name
              ORDER BY uel.userid
              )
,U2 AS (
             SELECT U1.userid ,U1.logins_total_num,
             U1.name AS last_login_platform_name,
             DATE(u1.ts) AS last_login_date  
             FROM U1 WHERE rn =1
        ) ---2494987
,U3 AS (
            SELECT u.id,
            MAX(bump_dt) AS last_bump_date,
            CASE WHEN v.email IS NOT NULL THEN 'Y' ELSE 'N' END AS email_verified_flag,
            CASE WHEN v.phone IS NOT NULL THEN 'Y' ELSE 'N' END AS phone_verified_flag,
            DATE(v.email) AS email_verification_date,
            DATE(v.phone) AS phone_verification_date,
            CASE WHEN u.status ='BLOCKED' THEN 'Y' ELSE 'N' END AS user_blocked_flag,
            u.lastlogindate AS last_login_date
            FROM donedeal_prod.users u
            LEFT JOIN donedeal_prod.verification v ON v.userid = u.id
            LEFT JOIN donedeal_aggregate.fact_ad fa ON fa.nk_user_id =u.id
 ----WHERE u.id =43202
           GROUP BY u.id,v.email,v.phone,u.status,u.lastlogindate 
           ORDER BY u.id
           )
 ,U4 AS (
         WITH U4iq AS
         (SELECT 
         uel.userid ,
         DATE(uel.ts) AS user_blocked_date,
         ROW_NUMBER() OVER ( PARTITION BY uel.userid  ORDER BY uel.ts DESC ) AS rn
         FROM donedeal_prod.user_event_log uel
         WHERE event =3 ---(Blocked event)
         GROUP BY uel.userid ,uel.ts 
         ORDER BY  uel.userid)
         SELECT U4iq.userid,U4iq.user_blocked_date FROM U4iq WHERE rn=1
        )
 ,U5 AS (
         WITH US_AGG AS
      (
       SELECT nk_user_id,
       nk_ad_id,
       CASE WHEN create_ts IS NOT NULL THEN 'T' ELSE 'F' END AS create_ts_flag,
       CASE WHEN first_publish_ts IS NOT NULL THEN 'T' ELSE 'F' END AS first_publish_ts_flag,
       CASE WHEN nongenuine_dt IS NOT NULL THEN 'T' ELSE 'F' END AS nongenuine_dt_flag,
       reported_num,
       publish_ts 
FROM donedeal_aggregate.fact_ad fa
WHERE nk_user_id in  ('43202','3274208','1')
GROUP BY nk_user_id,nk_ad_id ,create_ts,first_publish_ts,nongenuine_dt,publish_ts,reported_num
ORDER BY nk_user_id 
      )
SELECT nk_user_id ,
       SUM(CASE WHEN US_AGG.create_ts_flag = 'T' THEN 1 ELSE 0 END) AS ads_placed_total_num,
       SUM(CASE WHEN US_AGG.first_publish_ts_flag = 'T' THEN 1 ELSE 0 END) AS ads_purchased_total_num,
       SUM(CASE WHEN US_AGG.nongenuine_dt_flag = 'T' THEN 1 ELSE 0 END) AS nongenuine_ads_total_num,
       SUM(US_AGG.reported_num) AS reported_ads_total_num,
       MAX(US_AGG.publish_ts) AS last_ad_publish_date
       FROM US_AGG
GROUP BY nk_user_id 
 ) 
 ,U6 AS (
         SELECT fa.nk_user_id,MAX(publish_ts) AS last_car_ad_publish_date
         FROM donedeal_aggregate.fact_ad fa  AND dim_category_id =132
GROUP BY  fa.nk_user_id
ORDER BY fa.nk_user_id
         )
SELECT U3.id,
        U3.last_bump_date,
        U3.email_verified_flag,
        U3.phone_verified_flag,
        U3.email_verification_date,
        U3.phone_verification_date,
        U3.user_blocked_flag,
        U3.last_login_date,
        U2.last_login_date,
        U2.last_login_platform_name,
        U2.logins_total_num,
        U4.user_blocked_date,
		U5.ads_placed_total_num,
		U5.ads_purchased_total_num,
		U5.nongenuine_ads_total_num,
		U5.last_ad_publish_date
 FROM U3
 LEFT JOIN U2 ON U2.userid = U3.id
 LEFT JOIN U4 ON U4.userid = U3.id
 LEFT JOIN U5 ON U5.nk_user_id = U3.id
 LEFT JOIN U6 ON U6.nk_user_id = U3.id
 ORDER BY U3.id ;
 
 
 
 ---------------------------
 
 
 --last_ad_publish_date
with F1 as (SELECT nk_user_id,
       dc.category,
       nk_ad_id,
       create_ts,
       first_publish_ts,
       nongenuine_dt,
       reported_num,
       publish_ts ,
       ROW_NUMBER() OVER ( PARTITION BY nk_user_id  ORDER BY publish_ts DESC ) AS rn
FROM donedeal_aggregate.fact_ad fa
Left join donedeal_aggregate.dim_category dc ON dc.dim_category_id =fa.dim_category_id
where nk_user_id in  ('43202','3274208','1')
group by nk_user_id,dc.category,nk_ad_id ,create_ts,first_publish_ts,nongenuine_dt,publish_ts,reported_num
order by nk_user_id )
SELECT nk_user_id ,max(publish_ts)as last_ad_publish_date from F1
group by nk_user_id ;


---ad_agg data

with F1 AS(
       SELECT nk_user_id,
       nk_ad_id,
       create_ts,
       case when create_ts is not null then 'T' else 'F' end as create_ts_flag,
       first_publish_ts,
       case when first_publish_ts is not null then 'T' else 'F' end as first_publish_ts_flag,
       nongenuine_dt,
       case when nongenuine_dt is not null then 'T' else 'F' end as nongenuine_dt_flag,
       reported_num,
       publish_ts ,
       ROW_NUMBER() OVER ( PARTITION BY nk_user_id  ORDER BY publish_ts DESC ) AS rn
FROM donedeal_aggregate.fact_ad fa
where nk_user_id in  ('43202','3274208','1')
group by nk_user_id,nk_ad_id ,create_ts,first_publish_ts,nongenuine_dt,publish_ts,reported_num
order by nk_user_id )
SELECT nk_user_id ,
       SUM(CASE WHEN F1.create_ts_flag = 'T' THEN 1 ELSE 0 END) AS ads_placed_total_num,
       SUM(CASE WHEN F1.first_publish_ts_flag = 'T' THEN 1 ELSE 0 END) AS ads_purchased_total_num,
       SUM(CASE WHEN F1.nongenuine_dt_flag = 'T' THEN 1 ELSE 0 END) AS nongenuine_ads_total_num,
       SUM(F1.reported_num) AS reported_ads_total_num
       from F1
group by nk_user_id ;


----------------------------------****************************--------------------------------------------


WITH US_AGG AS
      (
       SELECT nk_user_id,
       nk_ad_id,
       CASE WHEN create_ts IS NOT NULL THEN 'T' ELSE 'F' END AS create_ts_flag,
       CASE WHEN first_publish_ts IS NOT NULL THEN 'T' ELSE 'F' END AS first_publish_ts_flag,
       CASE WHEN nongenuine_dt IS NOT NULL THEN 'T' ELSE 'F' END AS nongenuine_dt_flag,
       reported_num,
       publish_ts 
FROM donedeal_aggregate.fact_ad fa
WHERE nk_user_id in  ('43202','3274208','1')
GROUP BY nk_user_id,nk_ad_id ,create_ts,first_publish_ts,nongenuine_dt,publish_ts,reported_num
ORDER BY nk_user_id 
      )
SELECT nk_user_id ,
       SUM(CASE WHEN US_AGG.create_ts_flag = 'T' THEN 1 ELSE 0 END) AS ads_placed_total_num,
       SUM(CASE WHEN US_AGG.first_publish_ts_flag = 'T' THEN 1 ELSE 0 END) AS ads_purchased_total_num,
       SUM(CASE WHEN US_AGG.nongenuine_dt_flag = 'T' THEN 1 ELSE 0 END) AS nongenuine_ads_total_num,
       SUM(US_AGG.reported_num) AS reported_ads_total_num,
       MAX(US_AGG.publish_ts) AS last_ad_publish_date
       FROM US_AGG
GROUP BY nk_user_id ;

-----------purchase query---


with T1 AS (SELECT nk_user_id,nk_purchase_source_id,
CASE WHEN dps.nk_purchase_source_id = 13 then 'Y' else 'N' END AS stored_stripe_card_used,
CASE WHEN dps.nk_purchase_source_id = 10 then 'Y' else 'N' END AS stored_paypal,
CASE WHEN dps.nk_purchase_source_id = 7 then 'Y' else 'N' END AS stored_card
FROM donedeal_aggregate.fact_purchase fp
INNER JOIN donedeal_aggregate.dim_purchase_source dps on dps.dim_purchase_source_id = fp.dim_purchase_source_id
where nk_user_id in  ('43202','3274208','1') 
order by nk_user_id),
T2 AS (SELECT nk_user_id ,
SUM(CASE WHEN T1.stored_stripe_card_used = 'Y' THEN 1 ELSE 0 END) AS stored_strip_card_purchase_num,
SUM(CASE WHEN T1.stored_paypal = 'Y' THEN 1 ELSE 0 END) AS stored_paypal_purchase_num,
SUM(CASE WHEN T1.stored_card = 'Y' THEN 1 ELSE 0 END) AS stored_card_purchase_num
from T1
group by nk_user_id)
SELECT T2.nk_user_id ,
       CASE WHEN stored_strip_card_purchase_num > 0 THEN 'Y' ELSE 'N' END AS stored_strip_card_used,
       CASE WHEN stored_paypal_purchase_num > 0 THEN 'Y' ELSE 'N' END AS stored_paypal_used,
       CASE WHEN stored_card_purchase_num >0 THEN 'Y' ELSE 'N' END AS stored_card_used
FROM T2
order by nk_user_id;




--------------------FINAL Query---
WITH U1 AS (
            SELECT 
              uel.userid ,
              uel.ts,s.name,
              ROW_NUMBER() OVER ( PARTITION BY uel.userid  ORDER BY uel.ts DESC ) AS rn,
              COUNT(*) OVER ( PARTITION BY uel.userid ) as logins_total_num
              FROM donedeal_prod.user_event_log uel 
              INNER JOIN donedeal_prod.user_event ue ON ue.id = uel.event
              INNER JOIN donedeal_prod.source s ON s.id = uel.source
              WHERE ue.name IN('Login') -----AND userid IN ('43202','3274208','1')
              GROUP BY userid,uel.ts,s.name
              ORDER BY uel.userid
              )
,U2 AS (
          SELECT U1.userid ,U1.logins_total_num,
             U1.name AS last_login_platform_name,
             DATE(u1.ts) AS last_login_date  
             FROM U1 WHERE rn =1
        ) ---2494987
,U3 AS (
            SELECT u.id,
            MAX(bump_dt) AS last_bump_date,
            CASE WHEN v.email IS NOT NULL THEN 'Y' ELSE 'N' END AS email_verified_flag,
            CASE WHEN v.phone IS NOT NULL THEN 'Y' ELSE 'N' END AS phone_verified_flag,
            DATE(v.email) AS email_verification_date,
            DATE(v.phone) AS phone_verification_date,
            CASE WHEN u.status ='BLOCKED' THEN 'Y' ELSE 'N' END AS user_blocked_flag,
            u.lastlogindate AS last_login_date
            FROM donedeal_prod.users u
            LEFT JOIN donedeal_prod.verification v ON v.userid = u.id
            LEFT JOIN donedeal_aggregate.fact_ad fa ON fa.nk_user_id =u.id
 ----WHERE u.id =43202
           GROUP BY u.id,v.email,v.phone,u.status,u.lastlogindate 
           ORDER BY u.id
           )
 ,U4 AS (
         WITH U4iq AS
         (SELECT 
         uel.userid ,
         DATE(uel.ts) AS user_blocked_date,
         ROW_NUMBER() OVER ( PARTITION BY uel.userid  ORDER BY uel.ts DESC ) AS rn
         FROM donedeal_prod.user_event_log uel
         WHERE event =3 ---(Blocked event)
         GROUP BY uel.userid ,uel.ts 
         ORDER BY  uel.userid)
         SELECT U4iq.userid,U4iq.user_blocked_date FROM U4iq WHERE rn=1
        )
 ,U5 AS (
         WITH US_AGG AS
      (
       SELECT nk_user_id,
       nk_ad_id,
       CASE WHEN create_ts IS NOT NULL THEN 'T' ELSE 'F' END AS create_ts_flag,
       CASE WHEN first_publish_ts IS NOT NULL THEN 'T' ELSE 'F' END AS first_publish_ts_flag,
       CASE WHEN nongenuine_dt IS NOT NULL THEN 'T' ELSE 'F' END AS nongenuine_dt_flag,
       reported_num,
       publish_ts 
FROM donedeal_aggregate.fact_ad fa
---WHERE nk_user_id in  ('43202','3274208','1')
GROUP BY nk_user_id,nk_ad_id ,create_ts,first_publish_ts,nongenuine_dt,publish_ts,reported_num
ORDER BY nk_user_id 
      )
SELECT nk_user_id ,
       SUM(CASE WHEN US_AGG.create_ts_flag = 'T' THEN 1 ELSE 0 END) AS ads_placed_total_num,
       SUM(CASE WHEN US_AGG.first_publish_ts_flag = 'T' THEN 1 ELSE 0 END) AS ads_purchased_total_num,
       SUM(CASE WHEN US_AGG.nongenuine_dt_flag = 'T' THEN 1 ELSE 0 END) AS nongenuine_ads_total_num,
       SUM(US_AGG.reported_num) AS reported_ads_total_num,
       MAX(US_AGG.publish_ts) AS last_ad_publish_date
       FROM US_AGG
GROUP BY nk_user_id 
 ) 
 ,U6 AS (
         SELECT fa.nk_user_id,MAX(publish_ts) AS last_car_ad_publish_date
         FROM donedeal_aggregate.fact_ad fa  WHERE dim_category_id =132
GROUP BY  fa.nk_user_id
ORDER BY fa.nk_user_id
         )
, U7 AS(
        WITH U7iq1 AS 
		(
		SELECT nk_user_id,nk_purchase_source_id,
              CASE WHEN dps.nk_purchase_source_id = 13 THEN 'Y' ELSE 'N' END AS stored_stripe_card_used,
              CASE WHEN dps.nk_purchase_source_id = 10 THEN 'Y' ELSE 'N' END AS stored_paypal,
              CASE WHEN dps.nk_purchase_source_id = 7 THEN 'Y' ELSE 'N' END AS stored_card
FROM donedeal_aggregate.fact_purchase fp
INNER JOIN donedeal_aggregate.dim_purchase_source dps on dps.dim_purchase_source_id = fp.dim_purchase_source_id
---where nk_user_id in  ('43202','3274208','1') 
order by nk_user_id
         ),
U7iq2 AS 
      (
	  SELECT nk_user_id ,
            SUM(CASE WHEN U7iq1.stored_stripe_card_used = 'Y' THEN 1 ELSE 0 END) AS stored_strip_card_purchase_num,
            SUM(CASE WHEN U7iq1.stored_paypal = 'Y' THEN 1 ELSE 0 END) AS stored_paypal_purchase_num,
            SUM(CASE WHEN U7iq1.stored_card = 'Y' THEN 1 ELSE 0 END) AS stored_card_purchase_num
FROM U7iq1
GROUP BY nk_user_id)
SELECT U7iq2.nk_user_id ,
       CASE WHEN stored_strip_card_purchase_num > 0 THEN 'Y' ELSE 'N' END AS stored_strip_card_used,
       CASE WHEN stored_paypal_purchase_num > 0 THEN 'Y' ELSE 'N' END AS stored_paypal_used,
       CASE WHEN stored_card_purchase_num >0 THEN 'Y' ELSE 'N' END AS stored_card_used
FROM U7iq2
ORDER BY nk_user_id
)
,U8 AS (
    WITH U8iq1 AS
     (
     SELECT nk_user_id ,
           CASE WHEN dim_category_id = 132 THEN 'Y' ELSE 'N' END AS car_ad_flag
      FROM donedeal_aggregate.fact_ad fa
     ),
    U8iq2 AS(
     SELECT nk_user_id ,
            SUM(CASE WHEN U8iq1.car_ad_flag = 'Y' THEN 1 ELSE 0 END) AS placed_car_ad_num
     FROM U8iq1 
     GROUP BY nk_user_id
  )
       SELECT U8iq2.nk_user_id ,
       CASE WHEN U8iq2.placed_car_ad_num > 0 THEN 'Y' ELSE 'N' END AS placed_car_ad_flag
       FROM U8iq2
      --- where nk_user_id in  ('43202','3274208','1')
      ORDER  by nk_user_id)
,U9 AS (
        WITH U9iq AS 
		(
		SELECT 
		nk_user_id,
		dc.category,
		publish_ts ,
        ROW_NUMBER() OVER ( PARTITION BY nk_user_id  ORDER BY publish_ts DESC ) AS rn
        FROM  donedeal_aggregate.fact_ad fa
        LEFT JOIN donedeal_aggregate.dim_category dc ON dc.dim_category_id = fa.dim_category_id 
        WHERE  publish_ts IS NOT NULL
        GROUP BY nk_user_id,nk_ad_id,dc.category,publish_ts
        ORDER BY nk_user_id  )
        SELECT U9iq.nk_user_id,U9iq.category AS last_ad_publish_category_name
        FROM U9iq WHERE rn=1
       )
SELECT U3.id,
        U3.user_blocked_flag,
        U4.user_blocked_date,
        U3.last_login_date,
        U2.last_login_platform_name,
        U2.logins_total_num,
        U5.ads_placed_total_num,
        U5.ads_purchased_total_num,
        U5.nongenuine_ads_total_num,
        U5.reported_ads_total_num,
        U5.last_ad_publish_date,
        U9.last_ad_publish_category_name,
        U3.last_bump_date,
        U8.placed_car_ad_flag,
        U6.last_car_ad_publish_date,
        U7.stored_strip_card_used,
        U7.stored_paypal_used,
        U7.stored_card_used,
        U3.email_verified_flag,
        U3.email_verification_date,
        U3.phone_verified_flag,
        U3.phone_verification_date
 FROM U3
 LEFT JOIN U2 ON U2.userid = U3.id
 LEFT JOIN U4 ON U4.userid = U3.id
 LEFT JOIN U5 ON U5.nk_user_id = U3.id
 LEFT JOIN U6 ON U6.nk_user_id = U3.id
 LEFT JOIN U7 ON U7.nk_user_id =U3.id
 LEFT JOIN U8 ON U8.nk_user_id = U3.id
 LEFT JOIN U9  ON U9.nk_user_id =U3.id
 ORDER BY U3.id ;
 

