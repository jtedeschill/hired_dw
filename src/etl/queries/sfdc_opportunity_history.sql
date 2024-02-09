SELECT
Id,
OpportunityId,
CreatedDate,
CreatedById,
Field,
NewValue,
OldValue,
CreatedBy.Name
FROM OpportunityFieldHistory
WHERE Field IN ('StageName', 'CloseDate','Amount','created','Created by lead convert','opportunityCreatedFromLead')
