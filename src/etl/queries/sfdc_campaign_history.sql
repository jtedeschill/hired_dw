SELECT
id,
contactid,
LeadId, 
CreatedDate,
CM_MQL__c,
CM_MQL_Date__c,
Campaign.Name,
Campaign.Type,
campaign.Intent_Type__c, 
Status, 
MQL_Owner_Role__c,
MQL_Owner__c,
LastModifiedDate,
HasResponded,
Account_ID__c,
LastModifiedBy.Name,  
LastModifiedById
FROM campaignmember
WHERE (Contact.RecordType.Name != 'Candidate' OR Contact.RecordType.Name = null)
AND (Lead.type__c != 'Job Seeker' OR Lead.type__c = null)
