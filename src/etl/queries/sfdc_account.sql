SELECT
  Id,
  CreatedDate,
  Name,
  Account_Segment__c,
  Scraped_JD_Signal_Core_Tech__c,
  Last_Updated_Open_Roles__c,
  No_of_Open_Tech_Roles__c,
  Most_Recent_Service_Contract_End_Date__c,
  Owner.name,
  Last_Activity_Date__c,
  DF_Account_Score__c,
  Employee_Count__c,
  Employee_Count_Source__c,
  Account_HQ_Country__c,
  Type,
  dfox__Stage__c,
  website,
  Reassignment_Reason__c,
  Industry_Group__c, 
  DF_Industry__c, 
  Industry,
  Sub_Industry__c,
  Most_Recent_IVR__c,
  Most_Recent_Position_Posted__c,
  Careers_Page_Link__c,
  Job_Scraping_Type__c,
  Description_of_Roles__c,
  Most_Recent_Platform_Service_Contract__c,
  Most_Recent_Service_Contract_Plan_Type__c,
  Last_Inbound_Email_Sent__c,
  Last_Outbound_Email_Sent__c,
  Account_Manager_Role__c,
  Account_Manager_Manager__c,
  Date_of_Churn__c,
  Do_Not_Contact_Reason__c,
  Suspension_Date__c
FROM ACCOUNT
