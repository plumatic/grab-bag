select date_to_week(sn.timestamp_dt) as week, 
       sn.service,
       sn.category,
       sn.notification_campaign,
       count(distinct sn.user_id) as unique_users,
       count(distinct sn.notification_id) as sent,
       count(distinct if(cn.link_type is not null AND cn.link_type = 'open', sn.user_id, null)) as unique_opens,
       count(distinct if(cn.link_type is not null AND cn.link_type = 'home', sn.user_id, null)) as unique_home_clicks,
       count(distinct if(cn.link_type is not null AND cn.link_type <> 'unsubscribe' and cn.link_type <> 'open', sn.user_id, null)) as unique_engagement_clickers,
       count(distinct if(cn.link_type is not null AND cn.link_type = 'unsubscribe', sn.user_id, null)) as unique_unsubscribe_clickers
from sent_notifications sn
left join clicked_notifications cn
on cn.notification_id = sn.id
group by 1, 2, 3, 4
