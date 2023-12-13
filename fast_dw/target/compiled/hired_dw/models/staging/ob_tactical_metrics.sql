


WITH
  Latest_MQL_Date AS (
  SELECT
    e.account_id,
    MAX(e.event_date) OVER (PARTITION BY e.account_id) AS account_latest_MQL_date,
    FIRST_VALUE(e.event_attribute_2) OVER (PARTITION BY e.account_id ORDER BY e.event_date DESC) AS mql_contact_id
  FROM
    `hired-393411.Hired.engagement_events` e
  WHERE
    e.event_type = 'MQL'
  GROUP BY
    e.account_id,
    e.event_date,
    e.event_attribute_2 ),
  ContactActivities AS (
  SELECT
    e.account_id,
    e.event_date AS activity_date,
    e.event_attribute AS subject,
    e.event_attribute_2 AS contact_id,
  --  e.event_attribute_3 AS description,
    e.event_attribute_4 AS activity_owner,
    e.event_type
  FROM
    `hired-393411.Hired.engagement_events` e
--  WHERE
 --   e.event_type IN ('Call',
 --     'Email',
  --    'LinkedIn')
 --   AND e.event_date >= '2023-07-01' 
 ),
  ContactLatestMQL AS (
  SELECT
    ca.contact_id,
    MAX(e.event_date) AS contact_last_mql_date
  FROM
    `hired-393411.Hired.engagement_events` e
  JOIN
    ContactActivities ca
  ON
    e.event_attribute_2 = ca.contact_id
  WHERE
    e.event_type = 'MQL'
  GROUP BY
    ca.contact_id ),
  Metrics AS (
  SELECT
    a.account_owner_name AS owner_name,
    a.account_id AS worked_ob_accounts,
    a.job_scraping_date,
    a.no_open_tech_roles,
    a.account_name,
    mql.account_latest_MQL_date,
    mql.mql_contact_id,
    act.contact_id AS related_contact_id,
    act.activity_date,
    act.subject AS activity_subject,
    act.activity_owner,
    act.event_type,
  --  act.description,
    cmql.contact_last_mql_date,
    (EXTRACT(minute
      FROM
        PARSE_TIME('%T', REGEXP_EXTRACT(subject, '[0-9]+:[0-9]+:[0-9]+')))) AS call_duration,
    MIN(act.activity_date) OVER (PARTITION BY a.account_id) AS first_activity_date,
    MIN(act.activity_owner) OVER (PARTITION BY a.account_id) AS first_activity_owner,
    MAX(e_sqo.event_attribute) AS SQO,
    MAX(e_cw.event_attribute) AS Closed_Won,
    MAX(e_sal.event_attribute) AS SAL,
    MAX(e_sal.event_attribute_2) AS SAL_name,
    MAX(e_sqo.event_date) AS sqo_date,
    MAX(e_cw.event_date) AS closed_won_date,
    MAX(e_sal.event_date) AS sal_date,
    MAX(e_access.event_date) AS access_date,
    MAX(e_access.event_attribute) AS ACCESS,
    sc.job_function,
    sc.job_level,
    sc.contact_disqualified_reason,
    sc.disqualified_date,
    sc.disqualified_open_field,
    sc.nurture_reason,
    sc.nurture_date,
    sc.nurture_open_field,
    sc.contact_status,
    CONCAT(sc.contact_first_name, ' ',sc.contact_last_name) AS related_contact_name,
    sc.is_usergem,
    a.account_segment,
    a.most_recent_ivr,
    a.most_recent_position_posted,
    CASE
      WHEN a.industry_group = 'Software & Services' OR a.df_industry IN ('Internet Software & Services', 'Software') THEN 'Tech'
    ELSE
    'Non-Tech'
  END
    AS tech_vs_nontech
  FROM
    `hired-393411.Hired.calendar` cal
  JOIN
    `hired-393411.Hired.sfdc_account` a
  ON
    TRUE
  LEFT JOIN
    ContactActivities act
  ON
    a.account_id = act.account_id
  LEFT JOIN
    Latest_MQL_Date mql
  ON
    a.account_id = mql.account_id
  LEFT JOIN
    ContactLatestMQL cmql
  ON
    act.contact_id = cmql.contact_id
  LEFT JOIN
    `hired-393411.Hired.sfdc_contact` sc
  ON
    act.contact_id = sc.contact_id
  LEFT JOIN
    `hired-393411.Hired.engagement_events` e_sqo
  ON
    a.account_id = e_sqo.account_id
    AND e_sqo.event_type = 'SQO'
    AND DATE(e_sqo.event_date) >= '2023-07-01'
  LEFT JOIN
    `hired-393411.Hired.engagement_events` e_cw
  ON
    a.account_id = e_cw.account_id
    AND e_cw.event_type = 'Closed Won'
    AND DATE(e_cw.event_date) >= '2023-07-01'
  LEFT JOIN
    `hired-393411.Hired.engagement_events` e_sal
  ON
    a.account_id = e_sal.account_id
    AND e_sal.event_type IN ('Subscription',
      'Subscription - No Plan Type',
      'Ignored')
    AND DATE(e_sal.event_date) >= '2023-07-01'
  LEFT JOIN
    `hired-393411.Hired.engagement_events` e_access
  ON
    a.account_id = e_access.account_id
    AND e_access.event_type IN ('Access')
    AND DATE(e_access.event_date) >= '2023-07-01'
  WHERE
    LOWER(a.account_owner_name) LIKE ANY ('%alex malti%',
      '%brett crites%',
      '%erica lloyd%',
      '%greg garcia%',
      '%jacqueline moncure%',
      '%kevin parks%',
      '%mea anton%',
      '%sarah clancey%',
      '%durham cox%',
      'paul gopinathan')
    AND (act.event_type IN ('Email',
        'Call',
        'LinkedIn'))
    AND NOT EXISTS (
    SELECT
      1
    FROM
      `hired-393411.Hired.sales_dashboard_v2`
    WHERE
      account_id = a.account_id )
  GROUP BY
    a.account_owner_name,
    a.account_id,
    mql.account_latest_MQL_date,
    mql.mql_contact_id,
    a.job_scraping_date,
    a.no_open_tech_roles,
    a.account_name,
    act.contact_id,
    act.activity_date,
    act.activity_owner,
    act.subject,
  --  act.description,
    act.event_type,
    cmql.contact_last_mql_date,
    sc.job_function,
    sc.job_level,
    sc.contact_disqualified_reason,
    sc.disqualified_date,
    sc.disqualified_open_field,
    sc.nurture_reason,
    sc.nurture_date,
    sc.nurture_open_field,
    sc.contact_status,
    related_contact_name,
    sc.is_usergem,
    a.account_segment,
    a.industry_group,
    a.df_industry,
    a.most_recent_ivr,
    a.most_recent_position_posted )
, final as (
SELECT
  m.worked_ob_accounts AS account_id,
  m.first_activity_date,
  m.account_latest_MQL_date,
  m.mql_contact_id AS latest_mql_contact_id,
  m.owner_name,
  m.job_scraping_date,
  m.no_open_tech_roles,
  m.account_name,
  m.SQO,
  m.Closed_Won,
  m.SAL,
  m.SAL_name,
  m.sqo_date,
  m.closed_won_date,
  m.sal_date,
  m.access,
  m.access_date,
  m.related_contact_id,
  m.activity_date,
  m.activity_owner,
  m.first_activity_owner,
  m.activity_subject,
--  m.description,
  m.call_duration,
  m.event_type,
  m.contact_last_mql_date,
  m.job_function,
  m.job_level,
  m.contact_disqualified_reason,
  m.disqualified_date,
  m.disqualified_open_field,
  m.nurture_reason,
  m.nurture_date,
  m.nurture_open_field,
  m.contact_status,
  m.related_contact_name,
  m.is_usergem,
  m.account_segment,
  m.tech_vs_nontech,
  m.most_recent_ivr,
  m.most_recent_position_posted,
  CASE 
    WHEN EXISTS (
      SELECT 1
      FROM `hired-393411.Hired.engagement_events` e
      WHERE e.account_id = m.worked_ob_accounts
        AND e.event_type IN ('Call', 'Email', 'LinkedIn')
        AND e.event_date >= '2023-07-01'
    ) THEN 'worked_accounts'
    ELSE 'new_accounts'
  END AS account_status
FROM
  Metrics AS m
WHERE
  (DATE_DIFF(m.first_activity_date, m.account_latest_MQL_date, DAY) >= 90
    OR m.account_latest_MQL_date IS NULL)
ORDER BY
  m.worked_ob_accounts,
  m.activity_date,
  m.owner_name)

SELECT
f.*
,CASE WHEN f.access_date >= f.first_activity_date THEN account_id END AS m_access
,concat('https://hired.lightning.force.com/lightning/r/Opportunity/',f.access,'/view',f.access) as m_access_link
,concat('https://hired.lightning.force.com/lightning/r/Account/',f.account_id,'/view',f.account_id) as m_account_link 
,CONCAT('https://hired.my.salesforce.com/',f.account_id,'?srPos=0&srKp=001',f.account_name) as m_account_link_classic 
,CASE 
	WHEN f.job_scraping_date is not null and f.account_latest_MQL_date is not null then 'Past MQL + Hiring Signal'
    WHEN f.job_scraping_date is not null then 'Hiring Signal'
    ELSE 'Other Outbound'
END as m_bucket
,((PARSE_NUMERIC(REGEXP_EXTRACT(activity_subject, '\\[([0-9]{2}):')) * 3600) +
(PARSE_NUMERIC(REGEXP_EXTRACT(activity_subject, '([0-9]{2})\\]')) * 60) +
PARSE_NUMERIC(REGEXP_EXTRACT(activity_subject,':[0-9]{2}:([0-9]{2})'))) 
AS m_call_duration
,concat('https://hired.lightning.force.com/lightning/r/Opportunity/',f.related_contact_id,'/view',f.related_contact_id) as m_contact_id
,concat('https://hired.lightning.force.com/lightning/r/Account/',f.related_contact_id,'/view',f.related_contact_name) as m_contact_link
,concat('https://hired.lightning.force.com/lightning/r/Opportunity/',f.Closed_Won,'/view',f.Closed_Won) as m_cw_link 
,CASE WHEN closed_won_date >= first_activity_date THEN account_id END as m_cws 
,CASE 
  WHEN EXTRACT(DAYOFWEEK FROM first_activity_date) >= 2 AND EXTRACT(DAYOFWEEK FROM first_activity_date) <= 6 THEN first_activity_date
  ELSE NULL
END m_first_activity_date_business_days 
,CASE WHEN most_recent_ivr >= first_activity_date THEN account_id END AS m_ivr 
,CASE 
        WHEN owner_name = 'Casino Rojas' THEN 'Akit Mistry'
        WHEN owner_name = 'Dariush (deact) Morid' THEN 'Akit Mistry'
        WHEN owner_name = 'Jamair Atkins (Deact)' THEN 'Akit Mistry'
        WHEN owner_name = 'Jessica (deact) Berestecki' THEN 'Sam Swatski'
        WHEN owner_name = 'Tyler (deact) Pullen' THEN 'Sam Swatski'
        WHEN owner_name = 'Talisha (deact) Brown' THEN 'Sam Swatski'
        WHEN owner_name = 'John (Deact) Park Narron' THEN 'Sam Swatski'
        WHEN owner_name = 'Rob Cicek' THEN 'Sam Swatski'
        WHEN owner_name = 'Rebecca (deact) Ramsden' THEN 'Akit Mistry'
        WHEN owner_name = 'Christopher (deact) Monk' THEN 'Akit Mistry'
        WHEN owner_name = 'Sophia Collins' THEN 'Akit Mistry'
        WHEN owner_name = 'Kellie Wragg' THEN 'Akit Mistry'
        WHEN owner_name = 'Durham Cox' THEN 'Akit Mistry'
        WHEN owner_name = 'Bryce Flocks' THEN 'Sam Swatski'
        WHEN owner_name = 'Caleb Heck' THEN 'Sam Swatski'
        WHEN owner_name = 'Sarah Clancey' THEN 'Akit Mistry'
        WHEN owner_name = 'Mea Anton' THEN 'Akit Mistry'
        WHEN owner_name = 'Greg Garcia' THEN 'Akit Mistry'
        WHEN owner_name = 'Sam Swatski' THEN 'Akit Mistry'
        WHEN owner_name = 'Erica Lloyd' THEN 'Akit Mistry'
        WHEN owner_name = 'Kevin Parks' THEN 'Sam Swatski'
        WHEN owner_name = 'Jacqueline Moncure' THEN 'Sam Swatski'
        WHEN owner_name = 'Brett Crites' THEN 'Sam Swatski'
        WHEN owner_name = 'Alex Malti' THEN 'Sam Swatski'
        WHEN owner_name = 'Paul Gopinathan' THEN 'Akit Mistry'
        ELSE 'Unknown Manager'
    END 
  AS m_manager 
  ,concat('https://hired.lightning.force.com/lightning/r/Contact/',f.latest_mql_contact_id,'/view',f.latest_mql_contact_id) as m_mql_contact_link 
  ,CASE WHEN most_recent_position_posted >= first_activity_date THEN account_id END as m_position_posted 
  ,CASE WHEN sal_date >= first_activity_date THEN account_id END as m_sal 
  ,concat('https://hired.lightning.force.com/lightning/r/Opportunity/',f.SAL,'/view',f.SAL) as m_sal_link 
  ,CONCAT('https://hired.my.salesforce.com/',SAL,'?srPos=0&srKp=001',SAL_name) as m_sal_link_classic 
  ,CASE WHEN sqo_date >= first_activity_date THEN account_id END as m_sqo 
  ,concat('https://hired.lightning.force.com/lightning/r/Opportunity/',f.SQO,'/view',f.SQO) as m_sqo_link
  ,CASE WHEN (date_diff(date(activity_date),date(sal_date),DAY) <0 OR sal_date IS NULL) THEN TRUE ELSE FALSE END  m_task_before_sal
  ,EXTRACT(DAYOFWEEK FROM first_activity_date) AS m_weekday_first_activity 
  ,CASE 
    WHEN EXISTS (
      SELECT 1
      FROM `hired-393411.Hired.engagement_events` e
      WHERE e.account_id = f.account_id
        AND e.event_type IN ('Call', 'Email', 'LinkedIn')
        AND e.event_date >= '2023-07-01'
    ) THEN 'worked_accounts'
    ELSE 'new_accounts'
  END AS account_status_1
FROM FINAL f