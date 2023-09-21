SELECT
Id,
OpportunityId,
CreatedDate,
CreatedById,
Field,
NewValue,
OldValue

FROM OpportunityFieldHistory
WHERE Field = 'StageName'