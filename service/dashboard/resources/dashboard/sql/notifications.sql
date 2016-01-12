select date_to_week(sn.timestamp_dt) as week, 
       sn.service,
       sn.category,
       sn.notification_campaign,
       count(distinct sn.user_id) as unique_emails,
       count(distinct sn.notification_id) as sent,
       sum(IF(cn.link_type = 'home', 1, 0)) as home_clicks,
       sum(IF(cn.link_type = 'story', 1, 0)) as story_clicks,
       sum(IF(cn.link_type = 'interest', 1, 0)) as interest_clicks,
       sum(IF(cn.link_type = 'explore', 1, 0)) as explore_clicks,
       sum(IF(cn.link_type = 'profile', 1, 0)) as profile_clicks,
       sum(IF(cn.link_type = 'unsubscribe', 1, 0)) as unsubscribe_clicks,
       count(distinct if(cn.link_type is not null, sn.notification_id, null)) as clicked_emails,
       count(distinct if(cn.link_type is not null, sn.user_id, null)) as unique_clicking_users,
       ( 100 * count(distinct if(cn.link_type is not null, sn.user_id, null)) ) / count(distinct sn.user_id) as percent_unique_clicking_users
from sent_notifications sn
left join clicked_notifications cn
on cn.notification_id = sn.id
group by 1, 2, 3, 4
