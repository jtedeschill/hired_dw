SELECT
id,
contactid,
CreatedDate,
FCRM__FCR_QR__c,
FCRM__FCR_QR_Date__c,
campaign.name,
campaign.type

FROM campaignmember
WHERE Contact.RecordType.Name != 'Candidate'