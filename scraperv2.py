import pandas as pd
import requests
from bs4 import BeautifulSoup
from random import choice
from time import sleep
from fake_headers import Headers

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#i was using this to rotate headers, ignore
# desktop_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
#                  'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
#                  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
#                  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14',
#                  'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
#                  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
#                  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
#                  'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
#                  'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
#                  'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0']
fields = ['Population',
'Population Density',
'Housing Units',
'Median Home Value',
'Land Area',
'Water Area',
'Occupied Housing Units',
'Median Household Income',
'White',
'Black Or African American',
'American Indian Or Alaskan Native',
'Asian',
'Native Hawaiian & Other Pacific Islander',
'Other Race',
'Two Or More Races',
'Husband Wife Family Households',
'Single Guardian',
'Singles',
'Singles With Roommate',
'Households without Kids',
'Households with Kids',
'In Occupied Housing Units',
'Correctional Facility For Adults',
'Juvenile Facilities',
'Nursing Facilities',
'Other Institutional',
'College Student Housing',
'Military Quarters',
'Other Noninstitutional',
'Owned Households With A Mortgage',
'Owned Households Free & Clear',
'Renter Occupied Households',
'Households Vacant',
'For Rent',
'Rented & Unoccupied',
'For Sale Only',
'Sold & Unoccupied',
'For Season Recreational Or Occasional Use',
'For Migrant Workers',
'Vacant For Other Reasons',
'Worked Full-time with Earnings',
'Worked Part-time with Earnings',
'No Earnings',
'Less than High School Diploma',
'High School Graduate',
"Associate's degree",
"Bachelor's degree",
"Master's degree",
'Professional school degree',
'Doctorate degree',
'Enrolled in Public School',
'Enrolled in Private School',
'Not Enrolled in School']
blanks = ['NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN',
'NaN']
datadict = {'0':fields,'1':blanks}
blankdf = pd.DataFrame(data=datadict)

def random_headers():
    return {'User-Agent': choice(desktop_agents),'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'}

zipData = pd.read_csv('ZHVI_SFHomesTimeSeries_CA_Zips.csv')
zipList = zipData['RegionName']
indicesToDelete = [0,3,4,5,6,8,9,12,13,17,19,20]
bigDF = pd.DataFrame()
#agent = {"User-Agent":'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}
loopcounter = 0

for zipcode in zipList:
    url = 'https://www.unitedstateszipcodes.org/' + str(zipcode)
    response = requests.get(
        url,
        proxies={
        "https": "http://493a0c941a9c41f0b28a5f197ca2e1e5:@proxy.crawlera.com:8010/",
        },
        verify=False
    )
    #print(html_content.text)
    try:
        df_list = pd.read_html(response.text) # this parses all the tables in webpages to a list
        x = [i for j, i in enumerate(df_list) if j not in indicesToDelete]
        container = pd.DataFrame()
        counter = 0
        for item in x:
            item.columns = ['0','1','2']
            if counter <= 1:
                item = item.drop('2',1)
            else:
                item = item.drop('1',1)
            counter+=1
            item.columns =['0','1']
            container = pd.concat([container,item],axis = 0,ignore_index = True)
        container['1'] = container['1'].str.replace('&percnt;','%',regex=True)
        container['ZipCode'] = zipcode
        bigDF = pd.concat([bigDF,container],axis=0,ignore_index = True)
        print("WebsiteNum:",loopcounter,"UniqueVal:",container.iloc[0,1])
        loopcounter+=1
    except:
        print("PARSE ERROR ON ZIP:", str(zipcode))
        blankdf['ZipCode'] = str(zipcode)
        bigDF = pd.concat([bigDF,blankdf],axis=0,ignore_index = True)
        loopcounter+=1

#bigDF.rename(columns={'0':'Descriptor','1':'Value'})
bigDF.to_csv('CAZipCodeData.csv', encoding='utf-8', index=False)
##
values = pd.read_csv('ZHVI_SFHomesTimeSeries_CA_Zips.csv')
bigData = pd.read_csv('CAZipCodeData.csv')
response = values[['RegionName','8/31/2020']]
response = response.rename(columns={'RegionName': 'ZipCode','8/31/2020':'Zestimate'})
newnew = pd.DataFrame()
groups = bigData.groupby(by = "ZipCode")
for name, group in groups:
    zipValue = group.iloc[0,2]
    group1 = group.transpose()
    group1 = group1.drop('ZipCode',axis=0)
    group1.insert(loc=0,column = 'ZipCode',value=zipValue)
    group1.iloc[0,0] = 'ZipCode'
    group1.columns = group1.iloc[0,:]
    group1 = group1.drop('0',axis=0)
    newnew = pd.concat([newnew,group1],axis=0,ignore_index = True)


newnew = newnew.merge(response, on='ZipCode', how='left')
newnew = newnew.dropna()
newnew.to_csv('CAZipCode_Data.csv', encoding='utf-8', index=False)
