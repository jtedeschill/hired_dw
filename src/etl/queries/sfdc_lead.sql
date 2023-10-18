SELECT
id,
CreatedDate,
Disqualified_Reasons__c, 
Entered_Disqualified_Date__c, 
howDidYouHearAboutUs__c, 
How_Did_You_Hear_About_Us_Reasons__c,
Nurture_Reasons__c,
Entered_Nurture_Date__c,
Status,
Disqualified_Other__c,
Nurture_Other__c,
type__c,
Owner.name,
Job_Function__c, 
Job_Level__c,
UserGems__Is_a_UserGem__c,
LD_BookIt_Log_ID__c,
Account_Segment__c

FROM Lead
WHERE (type__c != 'Job Seeker' OR type__c = NULL)
AND ConvertedContact.Id = NULL
