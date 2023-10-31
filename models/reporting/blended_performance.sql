{{ config (
    alias = target.database + '_blended_performance'
)}}

SELECT channel, date::date as date, date_granularity,
            COALESCE(SUM(spend),0) as spend, 
            COALESCE(SUM(paid_purchases),0) as paid_purchases, 
            COALESCE(SUM(paid_revenue),0) as paid_revenue,
            COALESCE(SUM(clicks),0) as clicks,
            COALESCE(SUM(impressions),0) as impressions,
            COALESCE(SUM(net_sales),0) as net_sales,
            COALESCE(SUM(first_orders),0) as first_orders,
            COALESCE(SUM(repeat_orders),0) as repeat_orders,
            COALESCE(SUM(sessions),0) as sessions
    FROM
        (SELECT 'Google' as channel, date, date_granularity, 
            -- paid
            spend, purchases as paid_purchases, revenue as paid_revenue, clicks, impressions, 
            -- shopify
            0 as net_sales, 0 as first_orders, 0 as repeat_orders,
            -- ga
            0 as sessions
        FROM {{ ref('googleads_campaign_performance') }}
        
        UNION ALL
        
        SELECT 'Facebook' as channel, date, date_granularity, 
            --paid 
            spend, purchases as paid_purchases, revenue as paid_revenue, link_clicks as clicks, impressions, 
            -- shopify
            0 as net_sales, 0 as first_orders, 0 as repeat_orders,
            -- ga
            0 as sessions
        FROM {{ ref('facebook_ad_performance') }}  
        
        UNION ALL
        
        SELECT 'Shopify' as channel, date, date_granularity, 
            --paid 
            0 as spend, 0 as paid_purchases, 0 as paid_revenue, 0 as clicks, 0 as impressions, 
            -- shopify
            net_sales, first_orders, repeat_orders,
            -- ga
            0 as sessions
        FROM {{ ref('shopify_sales') }} 
        
        UNION ALL
        
        SELECT 'Google Analytics' as channel, date, date_granularity, 
            --paid 
            0 as spend, 0 as paid_purchases, 0 as paid_revenue, 0 as clicks, 0 as impressions, 
            -- shopify
            0 as net_sales, 0 as first_orders, 0 as repeat_orders,
            -- ga
            sessions
        FROM {{ ref('ga4_performance_by_campaign') }}
    
        UNION ALL
        
        SELECT 'Google Analytics' as channel, date, date_granularity, 
            --paid 
            0 as spend, 0 as paid_purchases, 0 as paid_revenue, 0 as clicks, 0 as impressions, 
            -- shopify
            0 as net_sales, 0 as first_orders, 0 as repeat_orders,
            -- ga
            sessions
        FROM {{ ref('googleanalytics_performance_by_campaign') }})
    
    GROUP BY channel, date, date_granularity
