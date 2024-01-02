{{ config(materialized='table') }}

with ch as (
  -- Selecting MQL events
  select 
  campaign_id,
  campaign_name,
  campaign_type,
  contact_id,
  lead_id,
  mql,
  created_Date
  from `Hired.sfdc_campaign_history`
  where mql = true and (intent_type = 'Declared Intent'
        OR status = 'Meeting Requested')

),



inquiry as (
 
  select
  campaign_id,
  campaign_name,
  campaign_type,
  contact_id,
  lead_id,
  inquiry,
  created_date

  from `Hired.sfdc_campaign_history`
  where inquiry=true and campaign_id not in (select distinct campaign_id from ch)
),

c as (
  select
  contact_id,
  account_id,
  from `Hired.sfdc_contact`
),


lc as (
  -- lead created

  select 
  distinct
  lead_id,
  account_id,
  coalesce(lead_id,account_id) as coalesced_id,
  lead_created_date as event_date,
  "Lead Created" as event_type,
  contact_id as event_attribute,
  "" as event_attribute_2,
  "" as event_attribute_3,
  "" as event_attribute_4

  from `Hired.sfdc_lead`  l
  left join `Hired.sfdc_campaign_history` as ch using (lead_id)
  left join c using (contact_id)
),

cch as (
  select
  campaign_id,
  campaign_name,
  contact_id,
  lead_id,
  account_id,
  coalesce(account_id, lead_id) as coalesced_id,
  created_date as mql_date,
  from ch 
  left join c using (contact_id)
),

cci as (
  
  select
  campaign_id,
  campaign_name,
  contact_id,
  lead_id,
  account_id,
  coalesce(account_id,lead_id) as coalesced_id,
  created_date as inquiry_date,
  from inquiry 
  left join c using (contact_id)

),

ci as (
  select
  lead_id,
  account_id,
  coalesce(account_id,lead_id) as coalesced_id,
  inquiry_date as event_date,
  "Inquiry" as event_type,
  campaign_name as event_attribute,
  contact_id as event_attribute_2,
  "" as event_attribute_3,
  "" as event_attribute_4

  
  from cci
  
),
ce as (
  select
  lead_id,
  account_id,
  coalesced_id,
  mql_date as event_date,
  "MQL" as event_type,
  campaign_name as event_attribute,
  contact_id as event_attribute_2,
  "" as event_attribute_3,
  "" as event_attribute_4

  
  from cch
  
),

t as (
  select
  cast(null as string) lead_id,
  account_id,
  account_id as coalesced_id,
  created_date as event_date,
  CASE 
    WHEN task_subtype IN ('Call', 'Email') THEN task_subtype
    WHEN subject LIKE ('%LinkedIn%') THEN 'LinkedIn'
    ELSE 'Task' END AS event_type,
  subject as event_attribute,
  who_id as event_attribute_2,
  description as event_attribute_3,
  owner_name as event_attribute_4,
  from `Hired.sfdc_task`

),

tl as (
  -- tasks from leads
  select
  who_id as lead_id,
  cast(null as string) account_id,
  who_id as coalesced_id,
  created_date as event_date,
  CASE 
    WHEN task_subtype IN ('Call', 'Email') THEN task_subtype
    WHEN subject LIKE ('%LinkedIn%') THEN 'LinkedIn'
    ELSE 'Task' END AS event_type,
  subject as event_attribute,
  who_id as event_attribute_2,
  description as event_attribute_3,
  owner_name as event_attribute_4,
  from `Hired.sfdc_task`
  where who_id in (select distinct lead_id from `Hired.sfdc_lead`)

),

opp as (
  select
  cast(null as string) lead_id,
  account_id,
  account_id as coalesced_id,
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
  opportunity_name as event_attribute_2,
  "" as event_attribute_3,
  "" as event_attribute_4

  from `Hired.sfdc_opportunity`
),

cw as (
select
cast(null as string) lead_id,
account_id,
account_id as coalesced_id,
opp_history_created_date as event_date,
'Closed Won' as event_type,
opportunity_id as event_attribute,
opportunity_name as event_attribute_2,
"" as event_attribute_3,
"" as event_attribute_4
from `hired-393411.Hired.sfdc_opportunity_history`
left join `Hired.sfdc_opportunity` using (opportunity_id)
where 
opportunity_id in 
(select distinct opportunity_id from `hired-393411.Hired.sfdc_opportunity` where stage_name = 'Closed Won' and won_acv>0 and (opportunity_record_type != 'Renewal' AND opportunity_type != 'Renewal' AND opportunity_type != 'Rescue'))
qualify row_number() over (partition by opportunity_id order by opp_history_created_date desc) = 1
),
cl as (
select 
cast(null as string) lead_id,
account_id,
account_id as coalesced_id,
opp_history_created_date as event_date,
'Closed Lost' as event_type,
opportunity_id as event_attribute,
opportunity_name as event_attribute_2,
"" as event_attribute_3,
"" as event_attribute_4
from `hired-393411.Hired.sfdc_opportunity_history`
left join `Hired.sfdc_opportunity` using (opportunity_id)
where 
opportunity_id in 
( select distinct opportunity_id from `hired-393411.Hired.sfdc_opportunity` where stage_name = 'Closed Lost')
qualify row_number() over (partition by opportunity_id order by opp_history_created_date desc) = 1
),
ac as (
  select 
  cast(null as string) lead_id,
  account_id,
  account_id as coalesced_id,
  account_created_date as event_date,
  "Account Created" as event_type,
  account_id as event_attribute,
  "" as event_attribute_2,
  "" as event_attribute_3,
  "" as event_attribute_4
  from `Hired.sfdc_account`
),

sqo as (
SELECT
  cast(null as string) lead_id,
  account_id,
  account_id as coalesced_id,
  CASE
    WHEN (opp_history_new_value = 'Qualified' AND campaign_type IN ('Burst Campaign', 'Request a Demo', 'Other') AND (plan_type != 'Hired Access' OR plan_type IS NULL)) THEN opp_history_created_date
    WHEN (opp_history_new_value = 'Proposal'
    AND campaign_type IN ('Sign_Up') AND (plan_type != 'Hired Access' OR plan_type IS NULL)) THEN opp_history_created_date
    WHEN (opp_history_new_value = 'Negotiation' AND campaign_type IN (NULL, 'eBooks - Whitepapers - Case Studies', 'Event - Tradeshow - Conference', 'Mailer Campaign', 'Paid advertising', 'Referral', 'Webinar') AND (plan_type != 'Hired Access' OR plan_type IS NULL)) THEN opp_history_created_date
END
  AS event_date,
  "SQO" as event_type,
  opportunity_id as event_attribute,
  opportunity_name as event_attribute_2,
  "" as event_attribute_3,
  "" as event_attribute_4
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
  from ci
  union all
  select
  *
  from t
  union all
  select
  *
  from tl
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
  union all 
  select 
  *
  from lc
),

events_daily as (

select 
lead_id,
account_id,
coalesced_id,
event_date,
event_type,
event_attribute,
event_attribute_2,
event_attribute_3,
event_attribute_4,
GENERATE_UUID() as event_id,
lag(event_date) over w1 last_event_date,
lag(event_type) over w1 last_event_type,
row_number() over w1 as event_number,
date_diff(event_date, lag(event_date) over w1,day) as days_since_last_event,
count(*) over w2 total_events,
from events
where (account_id not in  ('001d000001jA5ggAAC','0010W00002Z4B7sQAF') or account_id is null) and events.event_date is not null
window w1 as (partition by coalesced_id order by events.event_date), w2 as (partition by coalesced_id)

order by 6 desc,2 desc

),

last_events as (
    select
  *,  
  date(event_date) as date,
  case when (date_diff(date(event_date),date(lag(event_date) over (partition by account_id order by event_date)),day) >30)  then 1 end as last_event
  from events_daily
  qualify last_event = 1

),

events_final as (
  select
  *,  
  date(event_date) as date
  from events_daily

  union all 

  select
  lead_id,
  account_id,
  coalesced_id,
  timestamp_add(event_date, interval 30 day) as event_date,
  'Stale Date' event_type,
  'Stale Event' event_attribute,
  'Stale Event' event_attribute_2,
  'Stale Event' event_attribute_3,
  'Stale Event' event_attribute_4,
  GENERATE_UUID() event_id,
  event_date last_event_date,
  event_type last_event_type,
  event_number + 1 event_number,
  30 days_since_last_event,
  total_events + 1 total_events,
  date_add(date(event_date), interval 30 day) as date
  from last_events
)


select * from events_final

-- where account_id in (select distinct account_id from ac) #where ac.event_date >= "2023-01-01")
order by account_id, event_date