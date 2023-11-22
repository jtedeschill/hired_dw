{{ config(materialized='table') }}

with ch as (
  select 
  campaign_id,
  campaign_name,
  campaign_type,
  contact_id,
  mql,
  created_Date
  from `Hired.sfdc_campaign_history`
  where mql = true and (intent_type = 'Declared Intent'
        OR status = 'Meeting Requested')
),

c as (
  select
  contact_id,
  account_id,
  from `Hired.sfdc_contact`
),

cch as (
  select
  campaign_id,
  campaign_name,
  contact_id,
  account_id,
  created_date as mql_date,
  from ch 
  left join c using (contact_id)
),

ce as (
  select
  account_id,
  mql_date as event_date,
  "MQL" as event_type,
  campaign_name as event_attribute,
  contact_id as event_attribute_2,
  "" as event_attribute_3
  -- campaign_name as ob
  
  from cch
--   where regexp_contains(lower(campaign_name), "demo|in-app|signup")
  
),

t as (
  select
  account_id,
  created_date as event_date,
  task_subtype as event_type,
  subject as event_attribute,
  who_id as event_attribute_2,
  description as event_attribute_3
  from `Hired.sfdc_task`
),

opp as (
  select
  account_id,
  opportunity_created_date as event_date,
   CASE 
    WHEN opportunity_record_type = 'Subscription' 
      AND opportunity_type = 'New Business' 
      AND plan_type IS NOT NULL 
      AND plan_type != 'Hired Access'
      THEN 'Subscription'
    WHEN plan_type = 'Hired Access' AND opportunity_record_type = 'Subscription' THEN 'Access'
    WHEN opportunity_record_type = 'Transaction' AND won_acv > 0 THEN 'Transaction - Access'
    WHEN opportunity_record_type = 'Transaction' AND (won_acv = 0 OR won_acv IS NULL) THEN 'Transaction - Subscription'
    WHEN (opportunity_record_type = 'Renewal' OR opportunity_type = 'Renewal' OR opportunity_type = 'Rescue') THEN 'Renewal'
    WHEN (opportunity_record_type = 'Subscription' AND plan_type IS NULL) THEN 'Subscription - No Plan Type'
    ELSE 'Ignored'end event_type,
  opportunity_id as event_attribute,
  "" as event_attribute_2,
  "" as event_attribute_3

  from `Hired.sfdc_opportunity`
),

cw as (
select 
account_id,
opp_history_created_date as event_date,
'Closed Won' as event_type,
opportunity_id as event_attribute,
"" as event_attribute_2,
"" as event_attribute_3
from `hired-393411.Hired.sfdc_opportunity_history`
left join `Hired.sfdc_opportunity` using (opportunity_id)
where 
opportunity_id in 
( select distinct opportunity_id from `hired-393411.Hired.sfdc_opportunity` where stage_name = 'Closed Won' and won_acv>0)
qualify row_number() over (partition by opportunity_id order by opp_history_created_date desc) = 1
),
cl as (
select 
account_id,
opp_history_created_date as event_date,
'Closed Lost' as event_type,
opportunity_id as event_attribute,
"" as event_attribute_2,
"" as event_attribute_3
from `hired-393411.Hired.sfdc_opportunity_history`
left join `Hired.sfdc_opportunity` using (opportunity_id)
where 
opportunity_id in 
( select distinct opportunity_id from `hired-393411.Hired.sfdc_opportunity` where stage_name = 'Closed Lost')
qualify row_number() over (partition by opportunity_id order by opp_history_created_date desc) = 1
),
ac as (
  select 
  account_id,
  account_created_date as event_date,
  "Account Created" as event_type,
  account_id as event_attribute,
  "" as event_attribute_2,
  "" as event_attribute_3
  from `Hired.sfdc_account`
),

sqo as (
SELECT
  account_id,
  CASE
    WHEN (opp_history_new_value = 'Qualified' AND campaign_type IN ('Burst Campaign', 'Request a Demo', 'Other')) THEN opp_history_created_date
    WHEN (opp_history_new_value = 'Proposal'
    AND campaign_type IN ('Sign_Up')) THEN opp_history_created_date
    WHEN (opp_history_new_value = 'Negotiation' AND campaign_type IN (NULL, 'eBooks - Whitepapers - Case Studies', 'Event - Tradeshow - Conference', 'Mailer Campaign', 'Paid advertising', 'Referral', 'Webinar')) THEN opp_history_created_date
END
  AS event_date,
  "SQO" as event_type,
  opportunity_id as event_attribute,
  "" as event_attribute_2,
  "" as event_attribute_3
FROM
  `hired-393411.Hired.sfdc_opportunity_history`
LEFT JOIN
  `Hired.sfdc_opportunity`
USING
  (opportunity_id)
WHERE
  opp_history_field = 'StageName'
  AND opp_history_new_value IN ('Qualified',
    'Proposal',
    'Negotiation')
),

events as (
  select
  *
  from ce
  union all
  select
  *
  from t
  union all
  select
  *
  from opp
  union all
  select
  * 
  from sqo
  where event_date is not null
  union all
  select
  * 
  from cw
  union all
  select
  * 
  from cl
  union all
  select
  * 
  from ac
),

events_daily as (

select 

account_id,
event_date,
event_type,
event_attribute,
event_attribute_2,
event_attribute_3,
GENERATE_UUID() as event_id,
lag(event_date) over w1 last_event_date,
lag(event_type) over w1 last_event_type,
row_number() over w1 as event_number,
date_diff(event_date, lag(event_date) over w1,day) as days_since_last_event,
count(*) over w2 total_events,
from events
where account_id is not null and account_id not in  ('001d000001jA5ggAAC','0010W00002Z4B7sQAF') and events.event_date is not null
window w1 as (partition by account_id order by events.event_date), w2 as (partition by account_id)

order by 6 desc,2 desc

)

select * from events_daily

where account_id in (select distinct account_id from ac) #where ac.event_date >= "2023-01-01")
order by account_id, event_date