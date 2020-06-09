WITH countryList AS (
    SELECT country FROM `dhh-digital-analytics-dwh.pandora_analytics_ml.sessions` WHERE DATE(_PARTITIONTIME) BETWEEN '{dt}' AND '{dt_end}' AND country IS NOT NULL GROUP BY 1 ORDER BY COUNT(1) DESC LIMIT 17
),

fallbackCountryTbl AS (
    SELECT fullvisitorid, visitid, IF(geoNetwork.country='Myanmar (Burma)', 'Myanmar', geoNetwork.country) fallbackCountry FROM `foodoraanalyticspremium.103616390.ga_sessions_*` WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', '{dt}') AND FORMAT_DATE('%Y%m%d', '{dt_end}') GROUP BY 1,2,3 UNION ALL
    SELECT fullvisitorid, visitid, IF(geoNetwork.country='Myanmar (Burma)', 'Myanmar', geoNetwork.country) fallbackCountry FROM `bfd---foodora.138353473.ga_sessions_*` WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', '{dt}') AND FORMAT_DATE('%Y%m%d', '{dt_end}') GROUP BY 1,2,3 UNION ALL
    SELECT fullvisitorid, visitid, IF(geoNetwork.country='Myanmar (Burma)', 'Myanmar', geoNetwork.country) fallbackCountry FROM `bop---onlinepizza.111057681.ga_sessions_*` WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', '{dt}') AND FORMAT_DATE('%Y%m%d', '{dt_end}') GROUP BY 1,2,3 UNION ALL
    SELECT fullvisitorid, visitid, IF(geoNetwork.country='Myanmar (Burma)', 'Myanmar', geoNetwork.country) fallbackCountry FROM `bop---onlinepizza.111053874.ga_sessions_*` WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', '{dt}') AND FORMAT_DATE('%Y%m%d', '{dt_end}') GROUP BY 1,2,3 UNION ALL
    SELECT fullvisitorid, visitid, IF(geoNetwork.country='Myanmar (Burma)', 'Myanmar', geoNetwork.country) fallbackCountry FROM `bop---onlinepizza.110474939.ga_sessions_*` WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', '{dt}') AND FORMAT_DATE('%Y%m%d', '{dt_end}') GROUP BY 1,2,3
),

views_primer AS (
    SELECT
    'Pandora' entity, 
    date,
    CASE WHEN IFNULL(country,fallbackCountry) IN (SELECT country FROM countryList) THEN IFNULL(country,fallbackCountry) ELSE 'undefined' END country,
    CASE WHEN platform IN ('iOS', 'Android') THEN platform ELSE 'Web' END client,
    sessionId,
    REGEXP_REPLACE(REGEXP_REPLACE(LOWER((SELECT value FROM h.customDimensions WHERE index=170)), '^(.*?);|:(.*?);|:(.*?)$', ' -- '),'^ -- | -- $','') campaignNames,
    (SELECT value FROM h.customDimensions WHERE index=82) last_seen,
    COUNT(1) event_count

    FROM `dhh-digital-analytics-dwh.pandora_analytics_ml.sessions` t, t.hits h
    LEFT JOIN fallbackCountryTbl USING(fullVisitorId, visitid)

    WHERE DATE(_PARTITIONTIME) BETWEEN '{dt}' AND '{dt_end}'
    AND REGEXP_CONTAINS(lower(h.eventInfo.eventAction), 'carousel.swipe')
    GROUP BY 1,2,3,4,5,6,7
),

views AS (
    SELECT 
    entity, date, country, client, campaignName, 
    IFNULL(COUNT(DISTINCT sessionId),0) tile_views 
    FROM (
        SELECT entity, date, country, client, sessionId, campaignName 
        FROM views_primer, UNNEST(SPLIT(campaignNames,' -- ')) AS campaignName WITH OFFSET AS offset
        INNER JOIN (SELECT sessionId, max(last_seen) last_seen from views_primer GROUP BY 1) USING(sessionId, last_seen)
        WHERE CAST(offset AS INT64) <= (CAST(last_seen AS INT64)-1)
    )
    GROUP BY 1,2,3,4,5
),

tile_clicks as (
    SELECT
    'Pandora' entity, 
    date,
    CASE WHEN IFNULL(country,fallbackCountry) IN (SELECT country FROM countryList) THEN IFNULL(country,fallbackCountry) ELSE 'undefined' END country,
    CASE WHEN platform IN ('iOS', 'Android') THEN platform ELSE 'Web' END client,
    REGEXP_REPLACE(LOWER((SELECT value FROM h.customDimensions WHERE index=170)), '^(.*?)[;:]|:(.*?);|:(.*?)$', '') campaignName,
    IFNULL(COUNT(DISTINCT sessionId),0) tile_clicks 


    FROM `dhh-digital-analytics-dwh.pandora_analytics_ml.sessions` t, t.hits h
    LEFT JOIN fallbackCountryTbl USING(fullVisitorId, visitid)

    WHERE DATE(_PARTITIONTIME) BETWEEN '{dt}' AND '{dt_end}'
    AND REGEXP_CONTAINS(lower(h.eventInfo.eventAction), 'channel.clicked')
    GROUP BY 1,2,3,4,5
),

campaign_list as (
    SELECT 
    entity, date, country, client, campaignName, 
    IFNULL(COUNT(DISTINCT sessionId),0) campaign_list_loads,
    IFNULL(COUNT(DISTINCT IF(is_fallback,sessionId,NULL)),0) campaign_list_fallback,
    IFNULL(COUNT(DISTINCT IF(vendorQuantityShown<=3,sessionId,NULL)),0) campaign_list_less_then_3_shown

    FROM(
        SELECT
        'Pandora' entity, 
        date,   
        CASE WHEN IFNULL(country,fallbackCountry) IN (SELECT country FROM countryList) THEN IFNULL(country,fallbackCountry) ELSE 'undefined' END country,
        CASE WHEN platform IN ('iOS', 'Android') THEN platform ELSE 'Web' END client,
        sessionId,
        REGEXP_REPLACE(LOWER((SELECT value FROM h.customDimensions WHERE index=170)), '^(.*?)[;:]|:(.*?);|:(.*?)$', '') campaignName,
        REGEXP_CONTAINS(lower((SELECT value FROM h.customDimensions WHERE index=170)), ',(fallback)') is_fallback,
        MIN(SAFE_CAST((SELECT value FROM h.customDimensions WHERE index=34) AS INT64)) vendorQuantityShown

        FROM `dhh-digital-analytics-dwh.pandora_analytics_ml.sessions` t, t.hits h
        LEFT JOIN fallbackCountryTbl USING(fullVisitorId, visitid)

        WHERE DATE(_PARTITIONTIME) BETWEEN '{dt}' AND '{dt_end}' 
        AND REGEXP_CONTAINS(lower(h.eventInfo.eventAction), 'shop_list.updated')
        AND REGEXP_CONTAINS(lower((SELECT value FROM h.customDimensions WHERE index=105)), 'channel|referrer|deeplink')
        GROUP BY 1,2,3,4,5,6,7
    ) GROUP BY 1,2,3,4,5
),

vendor_clicks AS (
    SELECT
    'Pandora' entity, 
    date,   
    CASE WHEN IFNULL(country,fallbackCountry) IN (SELECT country FROM countryList) THEN IFNULL(country,fallbackCountry) ELSE 'undefined' END country,
    CASE WHEN platform IN ('iOS', 'Android') THEN platform ELSE 'Web' END client,
    REGEXP_REPLACE(LOWER((SELECT value FROM h.customDimensions WHERE index=170)), '^(.*?)[;:]|:(.*?);|:(.*?)$', '') campaignName,
    IFNULL(COUNT(DISTINCT sessionId),0) vendor_clicks 
    
    FROM `dhh-digital-analytics-dwh.pandora_analytics_ml.sessions` t, t.hits h
    LEFT JOIN fallbackCountryTbl USING(fullVisitorId, visitid)

    WHERE DATE(_PARTITIONTIME) BETWEEN '{dt}' AND '{dt_end}'
    AND REGEXP_CONTAINS(lower(h.eventInfo.eventAction), 'shop.clicked')
    AND REGEXP_CONTAINS(lower((SELECT value FROM h.customDimensions WHERE index=165)), 'channel|referrer|deeplink')
    GROUP BY 1,2,3,4,5
),

backendOrders AS (
    SELECT
    UPPER(CONCAT(common_name, order_code_google)) order_id,
    NOT(MAX(first_order+first_order_all)=0) first_order,
    IFNULL(SUM(CAST(gfv_eur as FLOAT64)),0) gfv_eur,
    IFNULL(SUM(CAST(gmv_eur as FLOAT64)),0) gmv_eur,
    IFNULL(SUM(items),0) items,
    FROM `dhh-digital-analytics-dwh.pandora_dwh_imports.il_global_v_fct_orders`
    LEFT JOIN `dhh-digital-analytics-dwh.pandora_dwh_imports.il_global_dim_countries` USING(rdbms_id)
    LEFT JOIN (SELECT rdbms_id, order_id, SUM(quantity) items FROM `dhh-digital-analytics-dwh.pandora_dwh_imports.il_global_v_fct_orderproducts` 
    WHERE CAST(order_date AS DATE) BETWEEN date_sub('{dt}', INTERVAL 3 DAY) AND date_add('{dt_end}', INTERVAL 3 DAY)
    GROUP BY 1,2) USING(rdbms_id, order_id)
    WHERE CAST(order_date AS DATE) BETWEEN date_sub('{dt}', INTERVAL 3 DAY) AND date_add('{dt_end}', INTERVAL 3 DAY)
    AND CONCAT(CAST(rdbms_id AS STRING), CAST(status_id AS STRING)) IN (SELECT CONCAT(CAST(rdbms_id AS STRING), CAST(status_id AS STRING)) FROM `dhh-digital-analytics-dwh.pandora_dwh_imports.il_global_v_meta_order_status` WHERE NOT(valid_order=0))
    GROUP BY 1
),

campaign_orders AS (
    SELECT 
    'Pandora' entity, 
    date,
    CASE WHEN IFNULL(country,fallbackCountry) IN (SELECT country FROM countryList) THEN IFNULL(country,fallbackCountry) ELSE 'undefined' END country,
    CASE WHEN platform IN ('iOS', 'Android') THEN platform ELSE 'Web' END client,
    campaignName,

    IFNULL(COUNT(DISTINCT IF(UPPER(CONCAT(IFNULL(country,fallbackCountry), h.transaction.transactionId)) IS NOT NULL AND vendorCode_click IS NOT NULL, sessionId, NULL)),0) 
    sessions_orders_carousel,
    IFNULL(COUNT(DISTINCT CASE WHEN vendorCode_click IS NOT NULL THEN UPPER(CONCAT(IFNULL(country,fallbackCountry), h.transaction.transactionId)) ELSE NULL END),0) orders_carousel,
    IFNULL(COUNT(DISTINCT CASE WHEN first_order AND vendorCode_click IS NOT NULL THEN UPPER(CONCAT(IFNULL(country,fallbackCountry), h.transaction.transactionId)) ELSE NULL END),0) orders_carousel_first,
    IFNULL(SUM(CASE WHEN vendorCode_click IS NOT NULL THEN gmv_eur ELSE NULL END),0) gmv_carousel,
    IFNULL(SUM(CASE WHEN vendorCode_click IS NOT NULL THEN gfv_eur ELSE NULL END),0) gfv_carousel,
    IFNULL(SUM(CASE WHEN vendorCode_click IS NOT NULL THEN items ELSE NULL END),0) items_carousel,

    FROM `dhh-digital-analytics-dwh.pandora_analytics_ml.sessions` t, t.hits h
    LEFT JOIN fallbackCountryTbl USING(fullVisitorId, visitid)
    LEFT JOIN backendOrders ON order_id = UPPER(CONCAT(IFNULL(country,fallbackCountry), h.transaction.transactionId))

    INNER JOIN (
        SELECT * FROM (
            SELECT 
            fullvisitorid shop_clicks_fullvisitorid, 
            visitid shop_clicks_visitid, 
            LOWER((SELECT value FROM h.customDimensions WHERE index=150)) vendorCode_click,
            LOWER(ARRAY_AGG((SELECT value FROM h.customDimensions WHERE index=165 AND NOT(value='NA')) ORDER BY h.time)[offset (0)]) vendorClickOrigin,
            MAX(REGEXP_REPLACE(LOWER((SELECT value FROM h.customDimensions WHERE index=170)), '^(.*?)[;:]|:(.*?);|:(.*?)$', '')) campaignName

            FROM `dhh-digital-analytics-dwh.pandora_analytics_ml.sessions` t, t.hits h
            WHERE DATE(_PARTITIONTIME) BETWEEN '{dt}' AND '{dt_end}'
            AND h.eventInfo.eventAction='shop.clicked'
            GROUP BY 1,2,3
        ) WHERE vendorClickOrigin IN ('deeplink','channel','referrer')
    ) shop_clicks ON t.fullvisitorid=shop_clicks.shop_clicks_fullvisitorid AND t.visitid = shop_clicks.shop_clicks_visitid AND REGEXP_EXTRACT(h.transaction.transactionId, '^(.*)-')=shop_clicks.vendorCode_click

    WHERE DATE(_PARTITIONTIME) BETWEEN '{dt}' AND '{dt_end}'
    AND h.eventInfo.eventAction = 'transaction' 
    GROUP BY 1,2,3,4,5
)


SELECT * FROM views 
LEFT JOIN tile_clicks USING(entity, date, country, client, campaignName)
LEFT JOIN campaign_list USING(entity, date, country, client, campaignName)
LEFT JOIN vendor_clicks USING(entity, date, country, client, campaignName)
LEFT JOIN campaign_orders USING(entity, date, country, client, campaignName)