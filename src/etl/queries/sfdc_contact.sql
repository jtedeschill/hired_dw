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
Entered_Disqualified__c, 
Disqualified_Other__c, 
Nurture_Reasons__c, 
Nurture_Other__c, 
Entered_Nurture__c, 
howDidYouHearAboutUs__c,
FCRM__FCR_Status__c,
owner.name,
owner.UserRole.name,
email,
No_Longer_at_Company__c

FROM CONTACT
WHERE RecordType.name != 'Candidate'
