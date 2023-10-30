{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
adset_name,
adset_id,
adset_effective_status,
audience,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
date,
date_granularity,
spend,
impressions,
link_clicks,
coalesce(add_to_cart,0)+coalesce("onsite_conversion.add_to_cart",0) as add_to_cart,
coalesce(purchases,0)+coalesce("onsite_conversion.purchase",0) as purchases,
coalesce(revenue,0)+coalesce("onsite_conversion.purchase_value",0) as revenue
FROM {{ ref('facebook_performance_by_ad') }}
