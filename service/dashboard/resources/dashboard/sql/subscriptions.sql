select date_to_week(timestamp_dt) as week, 
       category,
       if(subscribe = 0, 'UNSUBSCRIBE', 'SUBSCRIBE') as sub,
       sum(if(notification_type = 'all', 1, 0)) as _all_,
       sum(if(notification_type = 'reply', 1, 0)) as reply,
       sum(if(notification_type = 'update', 1, 0)) as _update_,
       sum(if(notification_type = 'digest', 1, 0)) as digest,
       sum(if(notification_type = 'social', 1, 0)) as social,
       sum(if(notification_type = 'comment-on-post', 1, 0)) as comment_on_post,
       sum(if(notification_type = 'at-mention', 1, 0)) as at_mention
from toggle_subscriptions 
group by 1, 2, 3;
