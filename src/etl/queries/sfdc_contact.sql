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
toLabel(Disqualified_Reasons__c),
Entered_Disqualified__c, 
Disqualified_Other__c, 
toLabel(Nurture_Reasons__c), 
Nurture_Other__c, 
Entered_Nurture__c, 
howDidYouHearAboutUs__c,
How_Did_You_Hear_About_Us_Reasons__c,
FCRM__FCR_Status__c,
owner.name,
owner.UserRole.name,
email,
No_Longer_at_Company__c,
UserGems__Is_a_UserGem__c,
LD_BookIt_Log_ID__c,
Assigned_To__r.Name,
utm_campaign__c,
Original_UTM_Campaign__c, 
utm_source__c,
Original_UTM_Source__c, 
utm_medium__c, 
Original_UTM_Medium__c, 
utm_content__c, 
Original_UTM_Content__c, 
UTM_Description__c, 
Original_UTM_Description__c,
Phone,
Left_or_inactive__c,
HasOptedOutOfEmail,
OutReach_OptOut__c,
Last_In_or_Outbound_Email__c,
Last_In_Person_Meeting__c,
Last_Inbound_Activity_Date__c,
Last_Inbound_Email_Sent__c,
Last_IVR_Date__c,
Last_Login_Date__c,
Last_Outbound_Activity_Date__c,
Last_Outbound_Email_Sent__c,
Last_Outbound_Email_Sent_AE__c,
Last_Outbound_Email_Sent_CSM__c,
Date_of_First_Activity__c,
Campaign_Type__c,
MQL__c,
MQL_Date__c
FROM CONTACT
WHERE RecordType.name != 'Candidate' 
