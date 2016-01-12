SELECT id as user_id,
       UNIX_TIMESTAMP(created)*1000 as created_date,
       (case created_client_type when 'iphone-browser' then 'web' else created_client_type end) as client
FROM datawarehouse_#{env}.users
WHERE created > '#{since}';
