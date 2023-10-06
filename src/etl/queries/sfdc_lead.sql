SELECT
id,
CreatedDate,
Disqualified_Reasons__c, 
Entered_Disqualified_Date__c, 
howDidYouHearAboutUs__c, 
Nurture_Reasons__c,
Entered_Nurture_Date__c,
Status,
Disqualified_Other__c,
Nurture_Other__c,
type__c

FROM Lead
WHERE (type__c != 'Job Seeker' OR type__c = NULL)
AND ConvertedContact.Id = NULL
