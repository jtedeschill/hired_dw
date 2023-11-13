SELECT
Id,
Account.id,
Who.Id,
What.Type,
What.Id,
CreatedDate, 
ActivityDate,
CompletedDateTime,
Subject,
Owner.Name,
owner.UserRole.name,
TaskSubtype,
CallDurationInSeconds,
CallDisposition,
Description

FROM Task
WHERE owner.name != 'Hired Admin'
AND owner.name != 'Marketo Sync'
AND (owner.userrole.name LIKE '%AE%'
    OR owner.userrole.name LIKE '%BDR%'
    OR owner.userrole.name LIKE '%SDR%')

