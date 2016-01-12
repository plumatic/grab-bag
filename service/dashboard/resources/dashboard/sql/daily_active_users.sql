SELECT DISTINCT user_id,
                UNIX_TIMESTAMP(date_ts)*1000 as date,
                (case client_type when 'iphone_browser' then 'web' else client_type end) as client
FROM analysis_#{env}.user_feed_fetch_daily_summary
WHERE date_ts > '#{since}';
