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
type,
SQO__c,
SQO_Date_Active__c

FROM OPPORTUNITY