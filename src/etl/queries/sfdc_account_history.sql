SELECT
id,
AccountId,
CreatedDate,
field,
OldValue,
NewValue

FROM AccountHistory
WHERE Field = 'Scraped_JD_Signal_Core_Tech__c'