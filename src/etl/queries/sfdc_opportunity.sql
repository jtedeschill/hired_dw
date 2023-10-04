SELECT
Id,
CreatedDate,
Account.Id,
ContactId,
Name,
StageName,
Last_Activity_Date__c,
Won_ACV__c,
SQL_Date__c,
owner.name,
owner.UserRole.name,
RecordType.name,
Plan_Type__c,
Deal_Type__c,
type,
SQO__c,
SQO_Date_Active__c,
CloseDate,
Campaign.type

FROM OPPORTUNITY
WHERE (Deal_Type__c != 'Trial' OR Deal_Type__c = NULL)
