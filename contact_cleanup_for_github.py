#deduping project, found around 1k duplicates out of 49k contacts (actually 933 out of 49,607)
#to make this run for yourself, input your credentials to create a connection to the salesforce api and manipulate the long string of contacts fields to fit your situation
#outputs are two csv files: contacts_with_scores.csv has all contact objects with individual duplicate scores and exact_contact_duplicates.csv, which has columns for the parent contact id, the older duplicates, the parent account id, and whether or not there are more than 3 duplicates


from simple_salesforce import Salesforce
from pandas import ExcelWriter
import pandas as pd
from pandas import DataFrame
sf = Salesforce(username='', password='', security_token=')

print('creating a pandas dataframe based on all active fields')
##a complete list of api field names taken from here https://www.youtube.com/watch?v=ucYszZ9sCSk plus copying and pasting
contact_api_fields = """zisf__ZoomInfo_Phone__c
ZoomInfo Phone
string (30)
(Blank)
More Setup
zisf__ZoomInfo_Email__c
ZoomInfo Email
email (80)
(Blank)
More Setup
zisf__zoom_lastupdated__c
ZoomInfo Last Updated
datetime
(Blank)
More Setup
zisf__zoom_id__c
ZoomInfo ID
string (20)
(Blank)
More Setup
Unique_ID__c
Unique ID
double (18, 0)
(Blank)
More Setup
Title
Title
string (128)
Consultant
More Setup
TimeZone__c
TimeZone
string (1300)*
EST
More Setup
SystemModstamp
System Modstamp
datetime, required
2019-03-26T17:15:11.000+0000
More Setup
Secondary_Email__c
Secondary Email
email (80)
(Blank)
More Setup
Salutation
Salutation
picklist (40)
(Blank)
More Setup
Role__c
Role
picklist (255)
(Blank)
More Setup
rh2__Integer_Test__c
Integer Test
double (3, 0)
(Blank)
More Setup
rh2__Formula_Test__c
Formula Test
currency (18, 0)*
0
More Setup
rh2__Describe__c
Describe
rh2__PS_Describe__c
(Blank)
More Setup
rh2__Currency_Test__c
Currency Test
currency (18, 0)
(Blank)
More Setup
ReportsToName
Reports To Name
string (121)
(Unknown)
More Setup
ReportsToId
Reports To ID
Contact
(Blank)
More Setup
RecordTypeId
Record Type ID
RecordType
012400000005iRwAAI
More Setup
Pull_Into_Gainsight__c
Pull Into Gainsight
boolean, required
(Unknown)
More Setup
Primary_Contact__c
Primary Contact
boolean, required
false
More Setup
PhotoUrl
Photo URL
url (255)
/services/images/photo/0031W000023terOQAQ
More Setup
Phone_Extension__c
Phone Extension
string (20)
(Blank)
More Setup
Phone
Work Phone
phone (40)
(617) 638-3777
More Setup
OwnerId
Owner ID
User
00540000003g14tAAA
More Setup
OtherStreet
Other Street
textarea (255)
(Blank)
More Setup
OtherState
Other State/Province
string (80)
(Blank)
More Setup
OtherPostalCode
Other Zip/Postal Code
string (20)
(Blank)
More Setup
OtherPhone
Other Phone
phone (40)
(Blank)
More Setup
OtherLongitude
Other Longitude
double (18, 15)
(Blank)
More Setup
OtherLatitude
Other Latitude
double (18, 15)
(Blank)
More Setup
OtherGeocodeAccuracy
Other Geocode Accuracy
picklist (40), restricted
(Blank)
More Setup
OtherCountry
Other Country
string (80)
(Blank)
More Setup
OtherCity
Other City
string (40)
(Blank)
More Setup
OtherAddress
Other Address
address
(Blank)
More Setup
OT_500__c
OT 500
string (1300)*
(Unknown)
More Setup
OpenEnrollmentTargetOct16__c
Open Enrollment Target (Oct16)
picklist (255)
(Unknown)
More Setup
Office_Location__c
Office Location
string (225)
(Blank)
More Setup
Nurture_Reasons__c
Nurture Reasons
picklist (255), dependent, restricted
(Blank)
More Setup
NotOwnedbyAcctOwner__c
Not Owned by Acct Owner
boolean, required*
false
More Setup
No_Longer_with_Company__c
No Longer with Company
boolean, required
true
More Setup
Name
Full Name
string (121), required
Amor Ibe
More Setup
Most_Recent_Campaign_Name__c
Most Recent Campaign Name
string (120)
(Blank)
More Setup
Most_Recent_Campaign_Interaction__c
Most Recent Campaign Interaction
datetime
(Blank)
More Setup
Most_Recent_Campaign_ID__c
Most Recent Campaign ID
string (18)
(Blank)
More Setup
MobilePhone
Mobile Phone
phone (40)
(Blank)
More Setup
mkto71_Original_Source_Type__c
Original Source Type
string (255)
salesforce.com
More Setup
mkto71_Original_Source_Info__c
Original Source Info
string (255)
Contact
More Setup
mkto71_Original_Search_Phrase__c
Original Search Phrase
string (255)
(Blank)
More Setup
mkto71_Original_Search_Engine__c
Original Search Engine
string (255)
(Blank)
More Setup
mkto71_Original_Referrer__c
Original Referrer
string (255)
(Blank)
More Setup
mkto71_Lead_Score__c
Lead Score
double (10, 0)
(Blank)
More Setup
mkto71_Inferred_State_Region__c
Inferred State Region
string (255)
(Blank)
More Setup
mkto71_Inferred_Postal_Code__c
Inferred Postal Code
string (255)
(Blank)
More Setup
mkto71_Inferred_Phone_Area_Code__c
Inferred Phone Area Code
string (255)
(Blank)
More Setup
mkto71_Inferred_Metropolitan_Area__c
Inferred Metropolitan Area
string (255)
(Blank)
More Setup
mkto71_Inferred_Country__c
Inferred Country
string (255)
(Blank)
More Setup
mkto71_Inferred_Company__c
Inferred Company
string (255)
(Blank)
More Setup
mkto71_Inferred_City__c
Inferred City
string (255)
(Blank)
More Setup
mkto71_Acquisition_Program_Id__c
Acquisition Program Id
double (18, 0)
(Blank)
More Setup
mkto71_Acquisition_Program__c
Acquisition Program
string (255)
(Blank)
More Setup
mkto71_Acquisition_Date__c
Acquisition Date
datetime
(Blank)
More Setup
mkto2__Original_Source_Type__c
Original Source Type
string (255)
(Unknown)
More Setup
mkto2__Original_Source_Info__c
Original Source Info
textarea (2000)
(Unknown)
More Setup
mkto2__Original_Search_Phrase__c
Original Search Phrase
string (255)
(Unknown)
More Setup
mkto2__Original_Search_Engine__c
Original Search Engine
string (255)
(Unknown)
More Setup
mkto2__Original_Referrer__c
Original Referrer
string (255)
(Unknown)
More Setup
mkto2__Lead_Score__c
Lead Score
double (18, 0)
(Blank)
More Setup
mkto2__Inferred_State_Region__c
Inferred State Region
string (255)
(Unknown)
More Setup
mkto2__Inferred_Postal_Code__c
Inferred Postal Code
string (255)
(Unknown)
More Setup
mkto2__Inferred_Phone_Area_Code__c
Inferred Phone Area Code
string (255)
(Unknown)
More Setup
mkto2__Inferred_Metropolitan_Area__c
Inferred Metropolitan Area
string (255)
(Unknown)
More Setup
mkto2__Inferred_Country__c
Inferred Country
string (255)
(Unknown)
More Setup
mkto2__Inferred_Company__c
Inferred Company
string (255)
(Unknown)
More Setup
mkto2__Inferred_City__c
Inferred City
string (255)
(Unknown)
More Setup
mkto2__Acquisition_Program_Id__c
Acquisition Program Id
double (18, 0)
(Unknown)
More Setup
mkto2__Acquisition_Program__c
Acquisition Program
string (255)
(Unknown)
More Setup
mkto2__Acquisition_Date__c
Acquisition Date
datetime
(Unknown)
More Setup
mkto_si__View_in_Marketo__c
View in Marketo
string (1300), html*
(Unknown)
More Setup
mkto_si__Urgency_Value__c
Urgency Value
double (4, 0)
(Blank)
More Setup
mkto_si__Urgency__c
Urgency
string (1300), html*
<img src="https://app.marketo.com/images/icons/fire0.png" alt="0" border="0"/>
More Setup
mkto_si__Sales_Insight__c
Sales Insight
string (1300), html*
(Unknown)
More Setup
mkto_si__Relative_Score_Value__c
Relative Score Value
double (4, 0)
(Blank)
More Setup
mkto_si__Relative_Score__c
Relative Score
string (1300), html*
<img src="https://app.marketo.com/images/icons/star0.png" alt="0" border="0"/>
More Setup
mkto_si__Priority__c
Priority
double (18, 0)
(Blank)
More Setup
mkto_si__Last_Interesting_Moment_Type__c
Last Interesting Moment Type
string (100)
(Blank)
More Setup
mkto_si__Last_Interesting_Moment_Source__c
Last Interesting Moment Source
string (100)
(Blank)
More Setup
mkto_si__Last_Interesting_Moment_Desc__c
Last Interesting Moment Desc
textarea (255)
(Blank)
More Setup
mkto_si__Last_Interesting_Moment_Date__c
Last Interesting Moment Date
datetime
(Blank)
More Setup
mkto_si__Last_Interesting_Moment__c
Last Interesting Moment
string (1300), html*
(Unknown)
More Setup
mkto_si__HideDate__c
Hide Date
date
(Unknown)
More Setup
MasterRecordId
Master Record ID
Contact
(Blank)
More Setup
Marketo_Sync__c
Marketo Sync
boolean, required
true
More Setup
Marketing_Influenced__c
Marketing Influenced
boolean, required
(Unknown)
More Setup
MailingStreet
Mailing Street
textarea (255)
(Blank)
More Setup
MailingState
Mailing State/Province
string (80)
(Blank)
More Setup
MailingPostalCode
Mailing Zip/Postal Code
string (20)
(Blank)
More Setup
MailingLongitude
Mailing Longitude
double (18, 15)
(Blank)
More Setup
MailingLatitude
Mailing Latitude
double (18, 15)
(Blank)
More Setup
MailingGeocodeAccuracy
Mailing Geocode Accuracy
picklist (40), restricted
(Blank)
More Setup
MailingCountry
Mailing Country
string (80)
(Blank)
More Setup
MailingCity
Mailing City
string (40)
(Blank)
More Setup
MailingAddress
Mailing Address
address
(Blank)
More Setup
LinkedIn_Profile__c
LinkedIn Profile
url (255)
(Blank)
More Setup
LID__LinkedIn_Member_Token__c
LinkedIn Member Token
string (80)
(Unknown)
More Setup
LID__LinkedIn_Company_Id__c
LinkedIn Company Id
string (80)
(Unknown)
More Setup
Level__c
Level
picklist (255), restricted
(Blank)
More Setup
LeadSource
Lead Source
picklist (40)
(Blank)
More Setup
Lead_Source_Detail__c
Lead Source Detail
string (50)
(Unknown)
More Setup
Lead_Lifecycle__c
Lead Lifecycle
picklist (255)
(Unknown)
More Setup
LastViewedDate
Last Viewed Date
datetime
2019-05-24T21:51:18.000+0000
More Setup
LastReferencedDate
Last Referenced Date
datetime
2019-05-24T21:51:18.000+0000
More Setup
LastName
Last Name
string (80), required
Ibe
More Setup
LastModifiedDate
Last Modified Date
datetime, required
2019-03-26T17:15:11.000+0000
More Setup
LastModifiedById
Last Modified By ID
User
0051W000005BZSgQAO
More Setup
LastCUUpdateDate
Last Stay-in-Touch Save Date
datetime
(Blank)
More Setup
LastCURequestDate
Last Stay-in-Touch Request Date
datetime
(Blank)
More Setup
LastActivityDate
Last Activity
date
(Blank)
More Setup
JigsawContactId
Jigsaw Contact ID
string (20)
(Blank)
More Setup
Jigsaw
Data.com Key
string (20)
(Unknown)
More Setup
IsEmailBounced
Is Email Bounced
boolean, required
false
More Setup
IsDeleted
Deleted
boolean, required
false
More Setup
Id
Contact ID
id (18), required
0031W000023terOQAQ
More Setup
HomePhone
Home Phone
phone (40)
(Blank)
More Setup
HasOptedOutOfFax
Fax Opt Out
boolean, required
(Unknown)
More Setup
HasOptedOutOfEmail
Email Opt Out
boolean, required
false
More Setup
geopointe__Geocode__c
Geocode
geopointe__Geocode__c
(Unknown)
More Setup
Function__c
Function
picklist (255), restricted
(Blank)
More Setup
FormPos__Current_Position_Start__c
Current Position Start
date
2018-05-07
More Setup
FirstName
First Name
string (40)
Amor
More Setup
First_Meeting_Scheduled__c
First Meeting Scheduled
date
(Unknown)
More Setup
First_Meeting_Completed__c
First Meeting Completed
date
(Blank)
More Setup
Fax
Business Fax
phone (40)
(Blank)
More Setup
engagio__Role__c
Role
string (128)
(Blank)
More Setup
engagio__FirstEngagementDate__c
First Engagement Date
datetime
(Blank)
More Setup
engagio__EngagementMinutesLast7Days__c
Engagement Minutes (7 days)
double (18, 0)
(Blank)
More Setup
engagio__EngagementMinutesLast3Months__c
Engagement Minutes (3 mo.)
double (18, 0)
(Blank)
More Setup
engagio__Department__c
Department
string (128)
(Blank)
More Setup
EmailBouncedReason
Email Bounced Reason
string (255)
(Blank)
More Setup
EmailBouncedDate
Email Bounced Date
datetime
(Blank)
More Setup
Email_Unsubscribe__c
Do Not Email
boolean, required
false
More Setup
Email_Address_Status__c
Email Address Status
picklist (255), restricted
Valid
More Setup
Email
Email
email (80)
(Blank)
More Setup
DSCORGPKG__Twitter_URL__c
Twitter URL
url (255)
(Unknown)
More Setup
DSCORGPKG__title_Custom__c
title_Custom
textarea (255)
(Unknown)
More Setup
DSCORGPKG__ReportsTo__c
DiscoverOrg ReportsTo
string (255)
(Unknown)
More Setup
DSCORGPKG__REMOVELinkedinURL__c
REMOVELinkedinURL
string (255)
(Unknown)
More Setup
DSCORGPKG__NickName__c
NickName
string (25)
(Unknown)
More Setup
DSCORGPKG__MiddleName__c
MiddleName
string (25)
(Unknown)
More Setup
DSCORGPKG__Management_Level__c
Management Level
string (255)
(Unknown)
More Setup
DSCORGPKG__Locked_By_User__c
Locked By User
User
(Unknown)
More Setup
DSCORGPKG__LinkedIn_URL__c
LinkedIn URL
url (255)
(Unknown)
More Setup
DSCORGPKG__Job_Function__c
Job Function
string (255)
(Unknown)
More Setup
DSCORGPKG__ITOrgChart__c
IT Org Chart
string (1300), html*
(Unknown)
More Setup
DSCORGPKG__External_DiscoverOrg_Id__c
DiscoverOrg Id
string (255), external id
(Unknown)
More Setup
DSCORGPKG__Exclude_Update__c
Exclude from DiscoverOrg Auto Updates
boolean, required
(Unknown)
More Setup
DSCORGPKG__Email_Invalid__c
Email Invalid
boolean, required
(Unknown)
More Setup
DSCORGPKG__DiscoverOrg_Technologies__c
DiscoverOrg Technologies
textarea (32768), html
(Unknown)
More Setup
DSCORGPKG__DiscoverOrg_State_Full_Name__c
DiscoverOrg State Full Name
string (50)
(Unknown)
More Setup
DSCORGPKG__DiscoverOrg_LastUpdate__c
DiscoverOrg Last Update
datetime
(Unknown)
More Setup
DSCORGPKG__DiscoverOrg_ID__c
DiscoverOrg ID
string (255)
(Unknown)
More Setup
DSCORGPKG__DiscoverOrg_FullCountryName__c
DiscoverOrg Country Full Name
string (50)
(Unknown)
More Setup
DSCORGPKG__DiscoverOrg_First_Update__c
DiscoverOrg First Update
datetime
(Unknown)
More Setup
DSCORGPKG__DiscoverOrg_Created_On__c
Created by DiscoverOrg
datetime
(Unknown)
More Setup
DSCORGPKG__DiscoverOrg_Company_ID__c
DiscoverOrg Company ID
string (255)
(Unknown)
More Setup
DSCORGPKG__department__c
DiscoverOrg Department
string (50)
(Unknown)
More Setup
DSCORGPKG__DeletedFromDiscoverOrg__c
DeletedFromDiscoverOrg
picklist (255)
(Unknown)
More Setup
DSCORGPKG__Conflict__c
Conflict
DSCORGPKG__Conflict__c
(Unknown)
More Setup
DSCORGPKG__Company_HQ_State_Full_Name__c
Company HQ State (Full Name)
string (255)
(Unknown)
More Setup
DSCORGPKG__Company_HQ_State__c
Company HQ State
string (255)
(Unknown)
More Setup
DSCORGPKG__Company_HQ_Postal_Code__c
Company HQ Postal Code
string (255)
(Unknown)
More Setup
DSCORGPKG__Company_HQ_Country_Full_Name__c
Company HQ Country (Full Name)
string (255)
(Unknown)
More Setup
DSCORGPKG__Company_HQ_Country_Code__c
Company HQ Country Code
string (255)
(Unknown)
More Setup
DSCORGPKG__Company_HQ_City__c
Company HQ City
string (255)
(Unknown)
More Setup
DSCORGPKG__Company_HQ_Address__c
Company HQ Address
string (255)
(Unknown)
More Setup
DQ_Reason__c
DQ Reason
picklist (255), dependent, restricted
(Blank)
More Setup
DoNotCall
Do Not Call
boolean, required
(Unknown)
More Setup
Do_Not_Sync_to_Marketo__c
Do Not Sync to Marketo
boolean, required
false
More Setup
Direct_Mail_Point_of_Contact__c
Direct Mail Point of Contact
boolean, required
(Unknown)
More Setup
Designated_Eligibility_Contact__c
Designated Eligibility Contact
boolean, required
(Unknown)
More Setup
Description
Contact Description
textarea (32000)
(Blank)
More Setup
Department
Department
string (80)
(Blank)
More Setup
Delete_Contact__c
Delete Contact
boolean, required
false
More Setup
Date_Edited_Created_by_ADR__c
Date Edited - Created by - ADR
date
(Blank)
More Setup
Data_Quality_Score__c
Data Quality Score
double (18, 0)*
40
More Setup
Data_Quality_Description__c
Data Quality Description
string (1300)*
Missing: Email, Complete Address, Salutation
More Setup
DaScoopComposer__Lookup_Phone__c
Lookup Phone
string (1300)*
(Unknown)
More Setup
DaScoopComposer__Lookup_Mobile__c
Lookup Mobile
string (1300)*
(Unknown)
More Setup
DaScoopComposer__Groove_Notes__c
Groove Notes
textarea (32768)
(Unknown)
More Setup
DaScoopComposer__Groove_Log_a_Call__c
Log a Call
string (1300)*
(Unknown)
More Setup
DaScoopComposer__Groove_LinkedIn__c
LinkedIn
string (1300)*
(Unknown)
More Setup
DaScoopComposer__Groove_Create_Opportunity__c
Create Opportunity
string (1300)*
(Unknown)
More Setup
DaScoopComposer__Email_Domain__c
Email Domain and Contact Hash
string (255), external id
(Unknown)
More Setup
DaScoopComposer__Email_2__c
Email 2
email (80)
(Unknown)
More Setup
DaScoopComposer__Domain__c
Domain
string (1300)*
(Unknown)
More Setup
DaScoopComposer__Black_List__c
Black List
DaScoopComposer__Black_List__c
(Unknown)
More Setup
CRMG_Map__Duplicated__c
Duplicated
double (18, 0)
(Unknown)
More Setup
CreatedDate
Created Date
datetime, required
2018-05-07T17:45:47.000+0000
More Setup
CreatedById
Created By ID
User
00540000003OMYEAA4
More Setup
Created_by_ADR__c
Created by - ADR
picklist (255)
(Blank)
More Setup
Contact_Subregion__c
Contact Subregion
string (50)
(Blank)
More Setup
Contact_Status__c
Contact Status
picklist (255), restricted
(Blank)
More Setup
Contact_Role__c
Contact Role
picklist (255), restricted
(Blank)
More Setup
Contact_Region__c
Contact Region
string (50)
(Blank)
More Setup
Contact_Lead_Status__c
Contact-Lead Status
picklist (255), restricted
(Blank)
More Setup
Contact_Last_Role__c
Contact's Last Role
Contact
(Blank)
More Setup
ConnectionSentDate
Sent Connection Date
datetime
(Unknown)
More Setup
ConnectionReceivedDate
Received Connection Date
datetime
(Unknown)
More Setup
CloudingoAgent__OTZ__c
Other Timezone
string (48)
(Blank)
More Setup
CloudingoAgent__ORDI__c
Other Residential Delivery Indicator
string (12)
(Blank)
More Setup
CloudingoAgent__OAV__c
Other Address Vacancy
string (1)
(Blank)
More Setup
CloudingoAgent__OAS__c
Other Address Status
double (18, 0)
0
More Setup
CloudingoAgent__OAR__c
Other Address Record Type
string (1)
(Blank)
More Setup
CloudingoAgent__MTZ__c
Mailing Timezone
string (48)
(Blank)
More Setup
CloudingoAgent__MRDI__c
Mailing Residential Delivery Indicator
string (12)
(Blank)
More Setup
CloudingoAgent__MAV__c
Mailing Address Vacancy
string (1)
(Blank)
More Setup
CloudingoAgent__MAS__c
Mailing Address Status
double (18, 0)
0
More Setup
CloudingoAgent__MAR__c
Mailing Address Record Type
string (1)
(Blank)
More Setup
CloudingoAgent__CES__c
Contact Email Status
double (18, 0)
0
More Setup
cirrusadv__Created_by_Cirrus_Insight__c
Created by Cirrus Insight
boolean, required
(Unknown)
More Setup
cbit__Twitter__c
Twitter
string (1300), html*
(Unknown)
More Setup
cbit__LinkedIn__c
LinkedIn
string (1300), html*
(Unknown)
More Setup
cbit__Facebook__c
Facebook
string (1300), html*
(Unknown)
More Setup
cbit__CreatedByClearbit__c
Created by Clearbit
boolean, required
(Unknown)
More Setup
cbit__ClearbitReady__c
ClearbitReady
boolean, required
(Unknown)
More Setup
cbit__Clearbit__c
Clearbit
cbit__Clearbit__c
(Unknown)
More Setup
Birthdate
Birthdate
date
(Blank)
More Setup
Benefits_Broker__c
Benefits Broker
boolean, required
false
More Setup
Asst_Email__c
Asst. Email
email (80)
(Blank)
More Setup
Associated_Opportunity__c
Associated Opportunity
Opportunity
(Blank)
More Setup
AssistantPhone
Asst. Phone
phone (40)
(Blank)
More Setup
AssistantName
Assistant's Name
string (40)
(Blank)
More Setup
Assistant_Name__c
Assistant Name
Contact
(Blank)
More Setup
AccountName
Account Name
string (255)
(Unknown)
More Setup
AccountId
Account ID
Account
0011W00001uP5HrQAK
More Setup
"""

#create a list from the string
first_api_list = contact_api_fields.split('\n')
#just bring in the api fieldnames
contact_api_fieldnames = []
count = 4
for term in first_api_list:
    count += 1
    if count % 5 == 0:
        contact_api_fieldnames.append(term)
    else:
        pass


#there was one pesky value at the end of the original list, this eliminates it from both
contact_api_fieldnames = contact_api_fieldnames[:-1]
first_api_list = first_api_list[:-1]

#query function
def SOQL(SOQL):
    qryResult = sf.query(SOQL)
    #print('Record Count {0}'.format(qryResult['totalSize']))
    isDone = qryResult['done'];

    if isDone == True:
        df = DataFrame(qryResult['records'])

    while isDone != True:
        try:
            if qryResult['done'] != True:
                df = df.append(DataFrame(qryResult['records']));
                qryResult = sf.query_more(qryResult['nextRecordsUrl'], True)
            else:
                df = df.append(DataFrame(qryResult['records']))
                isDone = True
                #print('completed')
                break;
        except NameError:
            df = DataFrame(qryResult['records'])
            qry = sf.query_more(qryResult['nextRecordsUrl'], True)

    df = df.drop('attributes', axis = 1)
    return df;

# Use if no textfile with api fieldnames
broken_api_fieldnames = []
working_api_fieldnames = []
for item in contact_api_fieldnames:
    try:
        SOQL('SELECT ' + item + ' FROM Contact')
        working_api_fieldnames.append(item)
    except:
        broken_api_fieldnames.append(item)


#get rid of the first item and comma
final_contact_api_string = ''
for item in working_api_fieldnames:
    final_contact_api_string = final_contact_api_string + ', ' + item
final_contact_api_string = final_contact_api_string[2:]


df = SOQL('SELECT '+ final_contact_api_string + ' FROM Contact')

#Trying to get account name from id, merge on id
account_fields = 'Name, Id'
account_df = SOQL('SELECT ' + account_fields + ' FROM Account')
account_df.columns = ['AccountName', 'AccountId']
merged_df = df.merge(account_df, how='left' , on= ['AccountId'])

merged_df.to_csv('ignore_contact_csv.csv', encoding='utf-8')


#!/usr/bin/env python
# coding: utf-8

# # setup and cleaning

# found 1000 duplicates out of 49,000, yay, see pre_june_3rd ipnyb for more info on what I found on missing fields


import pandas as pd
import string
import numpy as np
nan = np.nan
import math
import re

#read in csv, prelimary prep
df = pd.read_csv('ignore_contact_csv.csv', low_memory=False)
df.drop(df.columns[0], axis=1, inplace = True)
df = df.drop_duplicates()
df = df.replace({pd.np.nan: None})
df = df.replace({0: None})

#create a sub dataframe with only important fields
column_lst = df.columns.tolist()
def missing_values_table(df):
        mis_val = df.isnull().sum()
        mis_val_percent = 100 * df.isnull().sum() / len(df)
        mis_val_table = pd.concat([mis_val, mis_val_percent], axis=1)
        mis_val_table_ren_columns = mis_val_table.rename(
        columns = {0 : 'Missing Values', 1 : '% Missing of Total Values'})
        mis_val_table_ren_columns = mis_val_table_ren_columns[
            mis_val_table_ren_columns.iloc[:,1] != 0].sort_values(
        '% Missing of Total Values', ascending=False).round(1)
        #print ("Your selected dataframe has " + str(df.shape[1]) + " columns.\n"
        #    "There are " + str(mis_val_table_ren_columns.shape[0]) +
        #      " columns that have missing values.")
        return mis_val_table_ren_columns
missing_values_series = missing_values_table(df)
missing_values_series = missing_values_series.drop('Missing Values', 1)
missing_values_series['Fields'] = missing_values_series.index

#create a dataframe based on relevant fields e.g. those that are populated at least 35% of the time
missing_series_lst = missing_values_series['Fields'].tolist()
len(missing_series_lst)
missing_fields = []
for val in column_lst:
    if val not in missing_series_lst:
        missing_fields.append(val)
series_fields_under_65 = missing_values_series[missing_values_series['% Missing of Total Values'] <65]
rel_fields_list = series_fields_under_65['Fields'].tolist()
for var in missing_fields:
    rel_fields_list.append(var)
dup_df = df[rel_fields_list].copy()
dup_df['DuplicateScore'] = 0
dup_df['DuplicateIds'] = None

#clean name column
def strip_and_lower_names(inpt):
    s_inpt = str(inpt)
    s_inpt = s_inpt.translate(str.maketrans('', '', string.punctuation))
    inpt = s_inpt.translate(str.maketrans('', '', string.digits))
    return str(s_inpt).strip().lower()
dup_df['Name'] = dup_df['Name'].apply(strip_and_lower_names)

#create name+company column
#create name+state column
#create name+city column
dup_df['CompanyAndName'] = dup_df['Name'] + dup_df['AccountName']
dup_df['CityAndName'] = dup_df['Name'] + dup_df['MailingCity']
dup_df['StateAndName'] = dup_df['Name'] + dup_df['MailingState']

#clean email column
def lower_email(x):
    try:
        return x.lower()
    except:
        return x
dup_df['Email'] = dup_df['Email'].apply(lower_email)

def clean_phone_number(num):
    try:
        clean_phone_number = re.sub(r'[^0-9]+', '', num)
    except:
        clean_phone_number = None
    return clean_phone_number
dup_df['Phone'] = dup_df['Phone'].apply(clean_phone_number)

# # groupbys

#groupby name -> score = 80
name_df = dup_df.copy()
name_df = name_df[name_df['Name'] != None]
fullname_groups = name_df.groupby('Name').size().reset_index()
fullname_groups.columns = ['Name', 'Count']
fullname_duplicates = fullname_groups[fullname_groups['Count'] > 1]
fullname_duplicates_list = fullname_duplicates['Name'].tolist()
for val in fullname_duplicates_list:
    temp_df = dup_df[dup_df['Name'] == val]
    id_lst = temp_df['Id'].tolist()
    id_str = str(id_lst)
    for i in range(len(id_lst)):
        dup_df.loc[dup_df.Id == id_lst[i], 'DuplicateScore'] = 80
        dup_df.loc[dup_df.Id == id_lst[i], 'DuplicateIds'] = id_str


#groupby stateandname -> score = 90
stateandname_df = dup_df.copy()
stateandname_df = stateandname_df[stateandname_df['StateAndName'] != None]
stateandname_groups = stateandname_df.groupby('StateAndName').size().reset_index()
stateandname_groups.columns = ['StateAndName', 'Count']
stateandname_duplicates = stateandname_groups[stateandname_groups['Count'] > 1]
stateandname_duplicates_list = stateandname_duplicates['StateAndName'].tolist()
for val in stateandname_duplicates_list:
    temp_df = dup_df[dup_df['StateAndName'] == val]
    id_lst = temp_df['Id'].tolist()
    id_str = str(id_lst)
    for i in range(len(id_lst)):
        dup_df.loc[dup_df.Id == id_lst[i], 'DuplicateScore'] = 90
        dup_df.loc[dup_df.Id == id_lst[i], 'DuplicateIds'] = id_str



#by cityandname -> score = 95
cityandname_df = dup_df.copy()
cityandname_df = cityandname_df[cityandname_df['CityAndName'] != None]
cityandname_groups = cityandname_df.groupby('CityAndName').size().reset_index()
cityandname_groups.columns = ['CityAndName', 'Count']
cityandname_duplicates = cityandname_groups[cityandname_groups['Count'] > 1]
cityandname_duplicates_list = cityandname_duplicates['CityAndName'].tolist()
for val in cityandname_duplicates_list:
    temp_df = dup_df[dup_df['CityAndName'] == val]
    id_lst = temp_df['Id'].tolist()
    id_str = str(id_lst)
    for i in range(len(id_lst)):
        dup_df.loc[dup_df.Id == id_lst[i], 'DuplicateScore'] = 95
        dup_df.loc[dup_df.Id == id_lst[i], 'DuplicateIds'] = id_str



#exact email matches -> score = 100
#create an email_df, return a list of duplicate emails
email_df = dup_df.copy()
email_df = email_df[email_df['Email'] != None]
email_groups = email_df.groupby('Email').size().reset_index()
email_groups.columns = ['Email', 'Count']
email_group_duplicates = email_groups[email_groups['Count'] > 1]
email_group_duplicates_list = email_group_duplicates['Email'].tolist()
#take list of duplicate emails, return DuplicateScore = 100 and list of Ids that correspond
for val in email_group_duplicates_list:
    temp_df = dup_df[dup_df['Email'] == val]
    id_lst = temp_df['Id'].tolist()
    id_str = str(id_lst)
    for i in range(len(id_lst)):
        dup_df.loc[dup_df.Id == id_lst[i], 'DuplicateScore'] = 100
        dup_df.loc[dup_df.Id == id_lst[i], 'DuplicateIds'] = id_str



##exact linkedin matches -> score = 100
linkedin_df = dup_df.copy()
linkedin_df = linkedin_df[linkedin_df['cbit__LinkedIn__c'] != None]
linkedin_groups = linkedin_df.groupby('cbit__LinkedIn__c').size().reset_index()
linkedin_groups.columns = ['LinkedInUrls', 'Count']
linkedin_group_duplicates = linkedin_groups[linkedin_groups['Count'] > 1]
linkedin_group_duplicates_list = linkedin_group_duplicates['LinkedInUrls'].tolist()
for val in linkedin_group_duplicates_list:
    temp_df = dup_df[dup_df['cbit__LinkedIn__c'] == val]
    id_lst = temp_df['Id'].tolist()
    id_str = str(id_lst)
    for i in range(len(id_lst)):
        dup_df.loc[dup_df.Id == id_lst[i], 'DuplicateScore'] = 100
        dup_df.loc[dup_df.Id == id_lst[i], 'DuplicateIds'] = id_str



#exact companyandname matches -> score = 100
companyandname_df = dup_df.copy()
companyandname_df = companyandname_df[companyandname_df['CompanyAndName'] != None]
companyandname_groups = companyandname_df.groupby('CompanyAndName').size().reset_index()
companyandname_groups.columns = ['CompanyAndName', 'Count']
companyandname_duplicates = companyandname_groups[companyandname_groups['Count'] > 1]
companyandname_duplicates_list = companyandname_duplicates['CompanyAndName'].tolist()
for val in companyandname_duplicates_list:
    temp_df = dup_df[dup_df['CompanyAndName'] == val]
    id_lst = temp_df['Id'].tolist()
    id_str = str(id_lst)
    for i in range(len(id_lst)):
        dup_df.loc[dup_df.Id == id_lst[i], 'DuplicateScore'] = 100
        dup_df.loc[dup_df.Id == id_lst[i], 'DuplicateIds'] = id_str

# ## metrics thus far


print('there are at least ' + str(len(dup_df[dup_df['DuplicateScore'] == 100])) + ' exact duplicates out of ' + str(len(dup_df)) + ' total contacts.')
print('there are at least ' + str(len(dup_df[dup_df['DuplicateScore'] >= 95])) + ' potential duplicates with a score of 95 out of ' + str(len(dup_df)) + ' total contacts.')
print('there are at least ' + str(len(dup_df[dup_df['DuplicateScore'] >= 90])) + ' potential duplicates with a score of 90 out of ' + str(len(dup_df)) + ' total contacts.')
print('there are at least ' + str(len(dup_df[dup_df['DuplicateScore'] >= 80])) + ' potential duplicates with a score of 80 out of ' + str(len(dup_df)) + ' total contacts.')

# ## manipulating data for sunil's csv

#take exact matches, groupby groups
dup_df_over_99 = dup_df[dup_df['DuplicateScore'] == 100]
exact_dup_df = dup_df_over_99.groupby('DuplicateIds').size().reset_index()
exact_dup_df['AccountId'] = None
exact_dup_df['MasterContactId'] = None
exact_dup_df['DuplicateContacts'] = None
exact_dup_df['IdsTuple'] = None
exact_dup_df['If_More_Than_3'] = False


#take string of duplicate ids, convert to a list then process to find if any lists have mroe than 3 values
def string_to_list(ex_str):
    ex_str = ex_str[1:-1]
    ex_str = re.sub("'", '', ex_str)
    ex_lst = ex_str.split(',')
    return ex_lst
exact_dup_df['IdList'] = exact_dup_df['DuplicateIds'].apply(string_to_list)
def len_more_than_3(ex_lst):
    if len(ex_lst) > 3:
        return True
    else:
        return False
exact_dup_df['If_More_Than_3'] = exact_dup_df['IdList'].apply(len_more_than_3)

def return_newest_id(ex):
    d = {}
    newest_date = [0,0,0]
    newest_id = ''
    for contact_id in ex:
        contact_id = contact_id.strip(' ')
        row = dup_df.loc[dup_df['Id'] == contact_id]
        what = row.CreatedDate.item()
        what_lst = what.split('-')
        yr = int(what_lst[0])
        mnth = int(what_lst[1])
        day = int(what_lst[2].split('T')[0])
        date_lst = [yr, mnth, day]
        d[contact_id] = date_lst
    for ky in d.keys():
        if d[ky][0] > newest_date[0]:
            newest_id = ky
            newest_date = d[ky]
        elif d[ky][0] < newest_date[0]:
            pass
        else:
            if d[ky][1] > newest_date[1]:
                newest_id = ky
                newest_date = d[ky]
            elif d[ky][1] < newest_date[1]:
                pass
            else:
                if d[ky][2] > newest_date[2]:
                    newest_id = ky
                    newest_date = d[ky]
                else:
                    pass
    return_str = ''
    for val in d.keys():
        if val != newest_id:
            if len(return_str) == 0:
                return_str = val
            else:
                return_str = return_str + ',' + val
        else:
            pass
    return (newest_id, return_str)
exact_dup_df['IdsTuple'] = exact_dup_df['IdList'].apply(return_newest_id)
exact_dup_df['MasterContactId'] = exact_dup_df['IdsTuple'].apply(lambda x : x[0])
exact_dup_df['DuplicateContacts'] = exact_dup_df['IdsTuple'].apply(lambda x : x[1])

#find master contact's corresponding account id
def find_account_id(ex):
    row = dup_df.loc[dup_df['Id'] == ex]
    return row['AccountId'].item()
exact_dup_df['AccountId'] = exact_dup_df['MasterContactId'].apply(find_account_id)

print('exporting exact matches to exact_contact_duplicates.csv')
exact_dup_df.to_csv('exact_contact_duplicates.csv', encoding='utf-8')
