SELECT
Id,
CreatedDate,
Account.Id,
recordtype.name,
firstname,
lastname,
Job_Function__c,
Job_Level__c,
Title,
Last_Activity_Date__c,
Disqualified_Reasons__c,
FCRM__FCR_Status__c,
owner.name,
owner.UserRole.name,
email

FROM CONTACT
WHERE RecordType.name != 'Candidate'