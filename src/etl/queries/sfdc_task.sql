SELECT
Id,
Account.id,
Who.Id,
What.Type,
What.Id,
ActivityDate,
CompletedDateTime,
Subject,
Owner.Name,
owner.UserRole.name,
TaskSubtype,
CallDurationInSeconds,
CallDisposition

FROM Task
WHERE CreatedDate >= LAST_N_DAYS:365
AND WHO.TYPE != 'Lead'
AND owner.name != 'Hired Admin'
AND owner.name != 'Marketo Sync'
AND owner.userrole.name LIKE '%AE%'