SELECT
id,
ContactId,
CreatedDate,
Field,
OldValue,
NewValue

FROM ContactHistory
WHERE Field IN ('MQL_Date__c', 'FCRM__FCR_Status__c', 'Marketing_Funnel_State__c')