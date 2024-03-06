SELECT
id,
contactid,
LeadId, 
CreatedDate,
fcrm__fcr_sqr__c,
fcrm__fcr_sqr_date__c,
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
LastModifiedById,
UTM_Campaign__c, 
UTM_Content__c, 
UTM_Source__c, 
UTM_Description__c, 
UTM_Medium__c
FROM campaignmember
WHERE (Contact.RecordType.Name != 'Candidate' OR Contact.RecordType.Name = null)
AND (Lead.type__c != 'Job Seeker' OR Lead.type__c = null)
