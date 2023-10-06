SELECT
id,
contactid,
LeadId, 
CreatedDate,
FCRM__FCR_QR__c,
FCRM__FCR_QR_Date__c,
campaign.name,
campaign.type

FROM campaignmember
WHERE (Contact.RecordType.Name != 'Candidate' OR Contact.RecordType.Name = null)
AND (Lead.type__c != 'Job Seeker' OR Lead.type__c = null)
