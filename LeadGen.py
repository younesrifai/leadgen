from linkedin_scraper import Person, actions
from linkedin_scraper import Company
from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
from simple_salesforce import Salesforce
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from selenium.webdriver.common.keys import Keys
import time
import mysql.connector
from datetime import date
import json
import logging
import pygsheets
import df2gspread as d2g

#QCInput

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('/Users/younes/Downloads/creds.json',scope)
client = gspread.authorize(credentials)

sheet = client.open("Mass Upload").worksheet('FILTERED IMPORTS (US)')

print('Starting Import of QCs')

LeadStatus = 'Backlog'
Region = sheet.col_values(1)
Country = sheet.col_values(2)
BDR = sheet.col_values(3)
BDRID = sheet.col_values(4)
Company = sheet.col_values(5)
Website = sheet.col_values(6)
PhoneNumber = sheet.col_values(7)
SalesNavURL = sheet.col_values(8)
KAMValidation = sheet.col_values(9)
KAM = sheet.col_values(10)
KAMID = sheet.col_values(11)
Vertical = sheet.col_values(12)
Subvertical = sheet.col_values(13)
Structure = sheet.col_values(14)

Region.pop(0)
Country.pop(0)
BDR.pop(0)
BDRID.pop(0)
Company.pop(0)
Website.pop(0)
PhoneNumber.pop(0)
SalesNavURL.pop(0)
KAM.pop(0)
KAMID.pop(0)
Vertical.pop(0)
Subvertical.pop(0)
Structure.pop(0)

for i in BDR:
    if len(SalesNavURL) < len(BDR):
        SalesNavURL.append('')
    else:
        continue

for i in BDR:
    if len(Structure) < len(BDR):
        Structure.append('')
    else:
        continue

for i in BDR:
    if len(PhoneNumber) < len(BDR):
        PhoneNumber.append('')
    else:
        continue

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="leadGen",
  port="3308"
)

today = date.today()
week_number = today.isocalendar()[1]

j = range(0,len(BDR))

for i in j:
    try:
        mycursor = mydb.cursor()
        mycursor.execute("SELECT ID, SalesNavURL, Week FROM QCInput WHERE Week=" + str(week_number))

        URL_salesnavigator = []
        InputID = []
        Week = []

        for x in mycursor:
            URL_salesnavigator.append(x[1])
            InputID.append(x[0])
            Week.append(x[2])

        if SalesNavURL[i] in URL_salesnavigator:
            continue
        else:
            mycursor = mydb.cursor()
            sql = "INSERT INTO QCInput (Week, LeadStatus, Region, Country, BDR, BDRID, Company, Website, PhoneNumber, SalesNavURL, KAMValidation, KAM, KAMID, Vertical, Subvertical, Structure) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (week_number, LeadStatus, Region[i], Country[i], BDR[i], BDRID[i], Company[i], Website[i], PhoneNumber[i], SalesNavURL[i], KAMValidation[i], KAM[i], KAMID[i], Vertical[i], Subvertical[i], Structure[i])
            mycursor.execute(sql, val)
            mydb.commit()
    except:
        continue

#SalesNav Scraping

driver = webdriver.Chrome()

#Input Linkedin credentials
email = ""
password = ""
actions.login(driver, email, password)

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="leadGen",
  port="3308"
)

mycursor = mydb.cursor()
mycursor.execute("SELECT ID, SalesNavURL, Week FROM QCInput WHERE Week=" + str(week_number) + " AND Processed = 'No'")

URL_salesnavigator = []
InputID = []
Week = []

for x in mycursor:
    URL_salesnavigator.append(x[1])
    InputID.append(x[0])
    Week.append(x[2])

print('Starting Sales Navigator Scraping')

j = range(0,len(InputID))

for i in j:
    try:
        driver.get(URL_salesnavigator[i])
        time.sleep(2)
        driver.get(URL_salesnavigator[i])
        p = range(1,3)
        mycursor = mydb.cursor()
        sql = "UPDATE QCInput SET Processed = 'Yes' WHERE ID = " + str(InputID[i])
        mycursor.execute(sql)
        mydb.commit()
        for o in p:
            time.sleep(8)
            driver.execute_script("window.scrollTo(0,1*document.body.scrollHeight/10)")
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0,2*document.body.scrollHeight/10)")
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0,3*document.body.scrollHeight/10)")
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0,4*document.body.scrollHeight/10)")
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0,5*document.body.scrollHeight/10)")
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0,6*document.body.scrollHeight/10)")
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0,7*document.body.scrollHeight/10)")
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0,8*document.body.scrollHeight/10)")
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0,9*document.body.scrollHeight/10)")
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0,10*document.body.scrollHeight/10)")
            time.sleep(2)
            content = driver.page_source
            soup = BeautifulSoup(content,'lxml')

            names = soup.find_all("dt", attrs={"class": "result-lockup__name"})
            titles = soup.find_all("dd", attrs={"class": "result-lockup__highlight-keyword"})
            companies = soup.find_all("span", attrs={"class": "result-lockup__position-company"})
            header = soup.find("h2", attrs={"class": "search-nav--title Sans-16px-black-90%-bold-open flex align-items-center"})

            try:
                QC = header.a.text
                mycursor = mydb.cursor()
                sql = "UPDATE QCInput SET Company = '" + QC + "' WHERE ID = " + str(InputID[i])
                mycursor.execute(sql)
                mydb.commit()
            except:
                pass

            k = range(0,len(names))

            page = o + 1

            for l in k:
                try:
                    name = names[l].text
                    fullname = name.replace("        ","").replace("\n","")
                    firstname = fullname.split()[0]
                    lastname = fullname.split()[1]
                    if lastname == '':
                        continue
                    else:
                        pass
                    title = titles[l].span.text
                    company = companies[l].a.span.text
                    company = company.replace("\n          ","").replace("\n        ","")
                    link = 'linkedin.com' + names[l].a.get('href')

                    mycursor = mydb.cursor()
                    sql = "INSERT INTO SalesNav (InputID, Week, Name, Company, Title, LeadLinkedin) VALUES (%s, %s, %s, %s, %s, %s)"
                    val = (InputID[i], Week[i], fullname, company, title, link)
                    mycursor.execute(sql, val)
                    mydb.commit()
                except:
                    continue

            try:
                python_button = driver.find_elements_by_xpath("//button[@aria-label='Navigate to page " + str(page) + "']")
                time.sleep(1)
                python_button[0].click()
            except: #If different languages on your profile
                try:
                    python_button = driver.find_elements_by_xpath("//button[@aria-label='Accéder à la page " + str(page) + "']")
                    time.sleep(1)
                    python_button[0].click()
                except:
                    break
    except:
        continue


#Domain names

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="leadGen",
  port="3308"
)

mycursor = mydb.cursor()
mycursor.execute("SELECT Company, ID FROM QCInput WHERE Week=" + str(week_number))

company = []
companyID = []

for x in mycursor:
    company.append(x[0])
    companyID.append(x[1])

print('Starting Domain Search')

j = range(0,len(company))

for i in j:
    mycursor = mydb.cursor()
    mycursor.execute("SELECT CompanyID FROM Domains")

    DomainQCID= []

    for x in mycursor:
        DomainQCID.append(x[0])

    if companyID[i] in DomainQCID:
        continue
    else:
        link = 'https://autocomplete.clearbit.com/v1/companies/suggest?query=' + str(company[i])
        r = requests.get(url = link)
        data = r.json()
        if len(data) == 0:
            domain = "not found"
        else:
            domain = data[0]['domain']
        mycursor = mydb.cursor()
        sql = "INSERT INTO Domains (CompanyID, Company, Domain) VALUES (%s, %s, %s)"
        val = (companyID[i], company[i], domain)
        mycursor.execute(sql, val)
        mydb.commit()

#Snovio Preparation

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="leadGen",
  port="3308"
)

mycursor = mydb.cursor()
mycursor.execute("SELECT Name, Company, InputID, ID FROM SalesNav WHERE Week=" + str(week_number) + " AND Processed = 'No'")

companylist = []
namelist = []
companyID = []
SalesNavID = []

for x in mycursor:
    namelist.append(x[0])
    companylist.append(x[1])
    companyID.append(x[2])
    SalesNavID.append(x[3])

j = range(0,len(namelist))

print('Starting Snovio Table Preparation')

for i in j:
    mycursor = mydb.cursor()
    mycursor.execute('SELECT Domain FROM Domains WHERE CompanyID="' + str(companyID[i]) + '"')
    for x in mycursor:
        domain = x[0]
    firstname = namelist[i].split()[0]
    lastname = namelist[i].split()[1]
    mycursor = mydb.cursor()
    sql = "INSERT INTO Snovio (CompanyID, Week, Name, FirstName, LastName, Domain, SalesNavID) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (companyID[i], week_number, namelist[i], firstname, lastname, domain, SalesNavID[i])
    mycursor.execute(sql, val)
    mydb.commit()
    mycursor = mydb.cursor()
    sql = "UPDATE SalesNav SET Processed = 'Yes' WHERE ID = " + str(SalesNavID[i])
    mycursor.execute(sql)
    mydb.commit()

#Snovio Scraping

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="leadGen",
  port="3308"
)

mycursor = mydb.cursor()
mycursor.execute("SELECT ID, FirstName, LastName, Domain, Email, Processed FROM Snovio WHERE Week=" + str(week_number))

FirstName = []
LastName = []
Domain = []
ID = []
EmailsSnovio = []
Processed = []

for x in mycursor:
    ID.append(x[0])
    FirstName.append(x[1])
    LastName.append(x[2])
    Domain.append(x[3])
    EmailsSnovio.append(x[4])
    Processed.append(x[5])

#Snovio Credentials
client_id = '',
client_secret = ''

def get_access_token():
    params = {
        'grant_type':'client_credentials',
        'client_id':client_id,
        'client_secret': client_secret
    }
    res = requests.post('https://api.snov.io/v1/oauth/access_token', data=params)
    resText = res.text.encode('ascii','ignore')
    return json.loads(resText)['access_token']

def get_email_finder(domain, firstName, lastName):
    token = get_access_token()
    params = {'access_token':token,
              'domain': domain,
              'firstName': firstName,
              'lastName': lastName
    }
    res = requests.post('https://api.snov.io/v1/get-emails-from-names', data=params)
    return json.loads(res.text)

def add_names_to_find_emails(domain, firstName, lastName):
    token = get_access_token()
    params = {'access_token':token,
              'domain': domain,
              'firstName': firstName,
              'lastName': lastName
              }
    res = requests.post('https://api.snov.io/v1/add-names-to-find-emails', data=params)
    return json.loads(res.text)

j = range(0,len(FirstName))
templistsnovio = []
templistlusha = []
templisthunter = []

print('Starting Snovio Initialization of Search')

for i in j:
    if Processed[i] == 'No':
        try:
            add_names_to_find_emails(Domain[i], FirstName[i], LastName[i])
            mycursor = mydb.cursor()
            sql = "UPDATE Snovio SET Processed = 'Yes' WHERE ID = " + str(ID[i])
            mycursor.execute(sql)
            mydb.commit()
        except:
            pass
    else:
        continue

#print('Wait Time')

time.sleep(600)

#Lusha Scraping

print('Starting Lusha Email Search')

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="leadGen",
  port="3308"
)


mycursor = mydb.cursor()
mycursor.execute("SELECT Name, Company, InputID, ID FROM SalesNav WHERE Week=" + str(week_number))

companylist = []
namelist = []
companyID = []
SalesNavID = []

for x in mycursor:
    namelist.append(x[0])
    companylist.append(x[1])
    companyID.append(x[2])
    SalesNavID.append(x[3])

j = range(0,len(namelist))

mycursor = mydb.cursor()
mycursor.execute("SELECT Name FROM Lusha WHERE Week=" + str(week_number))

Name = []

for x in mycursor:
    Name.append(x[0])

for i in j:
    if namelist[i] in Name:
        continue
    else:
        firstname = namelist[i].split()[0]
        lastname = namelist[i].split()[1]
        mycursor = mydb.cursor()
        sql = "INSERT INTO Lusha (CompanyID, Week, Name, FirstName, LastName, Company, SalesNavID) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (companyID[i], week_number, namelist[i], firstname, lastname, companylist[i], SalesNavID[i])
        mycursor.execute(sql, val)
        mydb.commit()

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="leadGen",
  port="3308"
)

mycursor = mydb.cursor()
mycursor.execute("SELECT ID, FirstName, LastName, Company FROM Lusha WHERE Week=" + str(week_number) + " AND Processed = 'No'")

FirstName = []
LastName = []
Company = []
ID = []

for x in mycursor:
    ID.append(x[0])
    FirstName.append(x[1])
    LastName.append(x[2])
    Company.append(x[3])

j = range(0,len(FirstName))

for i in j:
    url = 'https://api.lusha.co/person?firstName=' + FirstName[i] + '&lastName=' + LastName[i] + '&company=' + Company[i]
    HEADERS_LUSHA={"api_key": ""}
    lusha = requests.get(url, headers=HEADERS_LUSHA)
    result = lusha.json()

    try:
        email = result["data"]["emailAddresses"][0]["email"]
    except:
        email = ''

    try:
        phone = result["data"]["phoneNumbers"][0]["internationalNumber"]
    except:
        phone = ''

    if email in templistlusha:
        templistlusha.append('')
        email = ''
    else:
        templistlusha.append(email)

    mycursor = mydb.cursor()
    sql = "UPDATE Lusha SET Email = '" + email + "', Phone = '" + phone + "', Processed = 'Yes' WHERE ID = " + str(ID[i])
    mycursor.execute(sql)
    mydb.commit()

#Hunter Scraping

print('Starting Hunter Email Search')

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="leadGen",
  port="3308"
)

mycursor = mydb.cursor()
mycursor.execute("SELECT Name, Company, InputID, ID FROM SalesNav WHERE Week=" + str(week_number))

companylist = []
namelist = []
companyID = []
SalesNavID = []

for x in mycursor:
    namelist.append(x[0])
    companylist.append(x[1])
    companyID.append(x[2])
    SalesNavID.append(x[3])

j = range(0,len(namelist))

mycursor = mydb.cursor()
mycursor.execute("SELECT Name FROM Hunter WHERE Week=" + str(week_number))

Name = []

for x in mycursor:
    Name.append(x[0])

for i in j:
    if namelist[i] in Name:
        continue
    else:
        firstname = namelist[i].split()[0]
        lastname = namelist[i].split()[1]
        mycursor = mydb.cursor()
        sql = "INSERT INTO Hunter (CompanyID, Week, Name, FirstName, LastName, Company, SalesNavID) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (companyID[i], week_number, namelist[i], firstname, lastname, companylist[i], SalesNavID[i])
        mycursor.execute(sql, val)
        mydb.commit()

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="leadGen",
  port="3308"
)

mycursor = mydb.cursor()
mycursor.execute("SELECT ID, Name, Company FROM Hunter WHERE Week=" + str(week_number) + " AND Processed='No'")

Name = []
Company = []
ID = []

for x in mycursor:
    ID.append(x[0])
    Name.append(x[1])
    Company.append(x[2])

j = range(0,len(Name))

HUNTER_API_KEY = ""

for i in j:
    try:
        url = 'https://api.hunter.io/v2/email-finder?company=' + Company[i] + '&full_name=' + Name[i] + '&api_key=' + HUNTER_API_KEY
        hunter = requests.get(url)
        result = hunter.json()
        try:
            score = result["data"]["score"]
        except:
            score = 0
        if score>=90:
            try:
                email = result["data"]["email"]
            except:
                email = ''
        else:
            email = ''
        try:
            phone = result["data"]["phone_number"]
            if phone == None:
                phone = ''
        except:
            phone = ''

        if email in templisthunter:
            templisthunter.append('')
            email = ''
        else:
            templisthunter.append(email)

        mycursor = mydb.cursor()
        sql = "UPDATE Hunter SET Email = '" + email + "', Phone = '" + phone + "', Processed = 'Yes' WHERE ID = " + str(ID[i])
        mycursor.execute(sql)
        mydb.commit()
    except:
        continue

#Snovio Part 2

print('Starting Snovio Final Search of Emails')

k = 0
l = 0

mycursor = mydb.cursor()
mycursor.execute("SELECT ID, FirstName, LastName, Domain, Email, Processed FROM Snovio WHERE Week=" + str(week_number))

FirstName = []
LastName = []
Domain = []
ID = []
EmailsSnovio = []
Processed = []

for x in mycursor:
    ID.append(x[0])
    FirstName.append(x[1])
    LastName.append(x[2])
    Domain.append(x[3])
    EmailsSnovio.append(x[4])
    Processed.append(x[5])

for i in j:
    if EmailsSnovio[i] != '' or EmailsSnovio[i] != None:
        try:
            result = get_email_finder(Domain[i], FirstName[i], LastName[i])
            time.sleep(2)
            email = result['data']['emails'][0]['email']
            status = result['data']['emails'][0]['emailStatus']

            if email in templistsnovio:
                templistsnovio.append('')
                email = ''
            else:
                templistsnovio.append(email)

            mycursor = mydb.cursor()
            sql = "UPDATE Snovio SET Email = '" + email + "', Status = '" + status + "' WHERE ID = " + str(ID[i])
            mycursor.execute(sql)
            mydb.commit()
            k = k + 1
        except:
            l = l + 1
            continue
    else:
        continue

print(str(k) + ' emails found in Snovio ...')
print(str(l) + ' not found or invalid')

#MassUpload Preparation

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="leadGen",
  port="3308"
)

print('Starting MassUpload Table Preparation')

mycursor = mydb.cursor()
mycursor.execute("SELECT CompanyID, FirstName, LastName, Email, Status, SalesNavID FROM Snovio WHERE Week=" + str(week_number))

CompanyID = []
FirstName = []
LastName = []
Email = []
Status = []
SalesNavID = []

for x in mycursor:
    CompanyID.append(x[0])
    FirstName.append(x[1])
    LastName.append(x[2])
    Email.append(x[3])
    Status.append(x[4])
    SalesNavID.append(x[5])


mycursor = mydb.cursor()
mycursor.execute("SELECT Email, Phone, SalesNavID FROM Lusha WHERE Week=" + str(week_number))

LushaEmail = []
LushaPhone = []
LushaID = []

for x in mycursor:
    LushaEmail.append(x[0])
    LushaPhone.append(x[1])
    LushaID.append(x[2])

mycursor = mydb.cursor()
mycursor.execute("SELECT Email, Phone, SalesNavID FROM Hunter WHERE Week=" + str(week_number))

HunterEmail = []
HunterPhone = []
HunterID = []

for x in mycursor:
    HunterEmail.append(x[0])
    HunterPhone.append(x[1])
    HunterID.append(x[2])

j = range(0,len(CompanyID))

mycursor = mydb.cursor()
mycursor.execute("SELECT SalesNavID FROM MassUpload WHERE Week=" + str(week_number))

SNIDs = []

for x in mycursor:
    SNIDs.append(x[0])

for i in j:
    if SalesNavID[i] in SNIDs:
        continue
    else:
        Email1 = ''
        Email2 = ''
        k = LushaID.index(SalesNavID[i])
        l = HunterID.index(SalesNavID[i])
        if Email[i] != '' and Email[i] != None:
            Email1 = Email[i]
            if LushaEmail[k] != '' and LushaEmail[k] != Email[i] and LushaEmail[k] != None:
                Email2 = LushaEmail[k]
                EmailSource = 'Snovio/Lusha'
            else:
                if HunterEmail[l] != '' and HunterEmail[l] != Email[i] and HunterEmail[k] != None:
                    Email2 = HunterEmail[l]
                    EmailSource = 'Snovio/Hunter'
                else:
                    Email2 = ''
                    EmailSource = 'Snovio'
        else:
            if LushaEmail[k] != '' and LushaEmail[k] != None:
                Email1 = LushaEmail[k]
                if HunterEmail[l] != '' and HunterEmail[l] != Email1 and HunterEmail[k] != None:
                    Email2 = HunterEmail[l]
                    EmailSource = 'Lusha/Hunter'
                else:
                    Email2 = ''
                    EmailSource = 'Lusha'
            else:
                if HunterEmail[l] != '' and HunterEmail[k] != None:
                    Email1 = HunterEmail[l]
                    EmailSource = 'Hunter'
                else:
                    Email1 = ''
                    Email2 = ''
                    EmailSource = 'N/A'

        mycursor = mydb.cursor()
        mycursor.execute("SELECT Title, LeadLinkedin FROM SalesNav WHERE ID ='" + SalesNavID[i] + "'")
        for x in mycursor:
            Title = x[0]
            LeadLinkedin = x[1]
        mycursor = mydb.cursor()
        mycursor.execute("SELECT LeadStatus, Region, Country, BDR, BDRID, Company, Website, PhoneNumber, SalesNavURL, KAMValidation, KAM, KAMID, Vertical, Subvertical, Structure FROM QCInput WHERE ID='" + str(CompanyID[i]) + "'")
        for x in mycursor:
            LeadStatus = x[0]
            Region = x[1]
            Country = x[2]
            BDR = x[3]
            BDRID = x[4]
            Company = x[5]
            Website = x[6]
            PhoneNumber = x[7]
            SalesNavURL = x[8]
            KAMValidation = x[9]
            KAM = x[10]
            KAMID = x[11]
            Vertical = x[12]
            Subvertical = x[13]
            Structure = x[14]
        if BDR == KAM:
            TeamSource = "KAM"
        else:
            TeamSource = "BDR"
        LeadSource = "Sales Navigator"
        mycursor = mydb.cursor()
        if len(SalesNavURL) > 254:
            SalesNavURL = SalesNavURL[0:253]
        if len(LeadLinkedin) > 254:
            LeadLinkedin = LeadLinkedin[0:253]
        if LushaPhone[i] == '' and HunterPhone[i] == '':
            PhoneSource = 'N/A'
        if LushaPhone[i] == '' and HunterPhone[i] != '':
            PhoneSource = 'Hunter'
        if LushaPhone[i] != '' and HunterPhone[i] == '':
            PhoneSource = 'Lusha'
        if LushaPhone[i] != '' and HunterPhone[i] != '':
            PhoneSource = 'Lusha/Hunter'
        sql = "INSERT INTO MassUpload (SalesNavID, CompanyID, Week, LeadStatus, Region, Country, BDR, BDRID, Company, CompanyLinkedin, KAMValidation, KAM, KAMID, Vertical, Subvertical, Structure, Website, LeadLinkedin, FirstName, LastName, Title, Email, Email2, Phone, Mobile, CompanyPhone, TerritoryPlan, LeadSource, TeamSource, EmailSource, PhoneSource) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (SalesNavID[i], CompanyID[i], week_number, LeadStatus, Region, Country, BDR, BDRID, Company, SalesNavURL, KAMValidation, KAM, KAMID, Vertical, Subvertical, Structure, Website, LeadLinkedin, FirstName[i], LastName[i], Title, Email1, Email2, HunterPhone[l], LushaPhone[k], '', '', LeadSource, TeamSource, EmailSource, PhoneSource)
        mycursor.execute(sql, val)
        mydb.commit()

#DiscoverOrg Import

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="leadGen",
  port="3308"
)

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('',scope) #insert file path
client = gspread.authorize(credentials)

sheet = client.open("Mass Upload").worksheet('Matched_Leads')

print('Starting DiscoverOrg Import')

LeadStatus = "Backlog"
Region = sheet.col_values(1)
Country = sheet.col_values(2)
BDR = sheet.col_values(3)
BDRID = sheet.col_values(4)
Company = sheet.col_values(5)
SalesNavURL = sheet.col_values(6)
KAMValidation = "TRUE"
KAM = sheet.col_values(7)
KAMID = sheet.col_values(8)
Vertical = sheet.col_values(9)
Subvertical = sheet.col_values(10)
Structure = sheet.col_values(11)
Website = sheet.col_values(12)
LeadLinkedin = sheet.col_values(13)
FirstName = sheet.col_values(14)
LastName = sheet.col_values(15)
Title = sheet.col_values(16)
Email = sheet.col_values(17)
Phone = sheet.col_values(18)
Mobile = sheet.col_values(19)
CompanyPhone = sheet.col_values(20)

Region.pop(0)
Country.pop(0)
BDR.pop(0)
BDRID.pop(0)
Company.pop(0)
SalesNavURL.pop(0)
KAM.pop(0)
KAMID.pop(0)
Vertical.pop(0)
Subvertical.pop(0)
Structure.pop(0)
Website.pop(0)
LeadLinkedin.pop(0)
FirstName.pop(0)
LastName.pop(0)
Title.pop(0)
Email.pop(0)
Phone.pop(0)
Mobile.pop(0)
CompanyPhone.pop(0)

i = 0

for i in BDR:
    if len(SalesNavURL) < len(BDR):
        SalesNavURL.append('')
    else:
        continue

for i in BDR:
    if len(Phone) < len(BDR):
        Phone.append('')
    else:
        continue

for i in BDR:
    if len(Email) < len(BDR):
        Email.append('')
    else:
        continue

for i in BDR:
    if len(Mobile) < len(BDR):
        Mobile.append('')
    else:
        continue

for i in BDR:
    if len(CompanyPhone) < len(BDR):
        CompanyPhone.append('')
    else:
        continue

for i in BDR:
    if len(Structure) < len(BDR):
        Structure.append('')
    else:
        continue

for i in BDR:
    if len(LeadLinkedin) < len(BDR):
        LeadLinkedin.append('')
    else:
        continue

LeadSource = "DiscoverOrg"

j = range(0,len(BDR))

for i in j:
    try:
        if BDR[i] == KAM[i]:
            TeamSource = "KAM"
        else:
            TeamSource = "BDR"
        LeadSource = "DiscoverOrg"
        mycursor = mydb.cursor()
        sql = "INSERT INTO MassUpload (Week, LeadStatus, Region, Country, BDR, BDRID, Company, CompanyLinkedin, KAMValidation, KAM, KAMID, Vertical, Subvertical, Structure, Website, LeadLinkedin, FirstName, LastName, Title, Email, Phone, Mobile, CompanyPhone, TerritoryPlan, LeadSource, TeamSource) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (week_number, LeadStatus, Region[i], Country[i], BDR[i], BDRID[i], Company[i], SalesNavURL[i], KAMValidation, KAM[i], KAMID[i], Vertical[i], Subvertical[i], Structure[i], Website[i], LeadLinkedin[i], FirstName[i], LastName[i], Title[i], Email[i], Phone[i], Mobile[i], CompanyPhone[i], '', LeadSource, TeamSource)
        mycursor.execute(sql, val)
        mydb.commit()
    except:
        continue

input("Press Enter to continue to Upload...")

#Upload

sf = Salesforce(username='',password='',security_token='')

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="leadGen",
  port="3308"
)

mycursor = mydb.cursor()
mycursor.execute("SELECT BDR, Region, ID FROM MassUpload WHERE Week=" + str(week_number) + " AND Uploaded IS NULL")

BDRs = []
Regions = []
MUID = []

for x in mycursor:
    BDRs.append(x[0])
    Regions.append(x[1])
    MUID.append(x[2])


BDRUnique = list(dict.fromkeys(BDRs))

j = range(0,len(BDRUnique))
l = range(0,len(BDRs))

print('Starting Upload to SalesForce')

for i in j:
    k = BDRs.index(BDRUnique[i])
    temp = Regions[k].replace(" ","") + " " + BDRUnique[i].replace(" ","") + " W" + str(week_number)
    camp = sf.Campaign.create({'Name':temp})
    campaign_id = camp['id']
    for m in l:
        if BDRs[m] == BDRUnique[i]:
            mycursor = mydb.cursor()
            sql = "UPDATE MassUpload SET CampaignID = '" + str(campaign_id) + "' WHERE ID = '" + str(MUID[m]) + "'"
            mycursor.execute(sql)
            mydb.commit()

mycursor = mydb.cursor()
mycursor.execute("SELECT ID, Region, Country, BDR, BDRID, Company, CompanyLinkedin, KAMValidation, KAM, KAMID, Vertical, Subvertical, Structure, Website, LeadLinkedin, FirstName, LastName, Title, Email, Phone, Mobile, CompanyPhone, TerritoryPlan, LeadSource, TeamSource, CampaignID, Uploaded, Email2, EmailSource, PhoneSource FROM MassUpload WHERE Week=" + str(week_number) + " AND Uploaded IS NULL")

ID = []
Region = []
Country = []
BDR = []
BDRID = []
Company = []
CompanyLinkedin = []
Validated = []
KAM = []
KAMID = []
Vertical = []
Subvertical = []
Structure = []
Website = []
LeadLinkedin = []
FirstName = []
LastName = []
Title = []
Email = []
Phone = []
Mobile = []
CompanyPhone = []
Territory = []
LeadSource = []
TeamSource = []
CampaignID = []
UploadedCol = []
Email2 = []
EmailSource = []
PhoneSource = []

for x in mycursor:
    ID.append(x[0])
    Region.append(x[1])
    Country.append(x[2])
    BDR.append(x[3])
    BDRID.append(x[4])
    Company.append(x[5])
    CompanyLinkedin.append(x[6])
    Validated.append(x[7])
    KAM.append(x[8])
    KAMID.append(x[9])
    Vertical.append(x[10])
    Subvertical.append(x[11])
    Structure.append(x[12])
    Website.append(x[13])
    LeadLinkedin.append(x[14])
    FirstName.append(x[15])
    LastName.append(x[16])
    Title.append(x[17])
    Email.append(x[18])
    Phone.append(x[19])
    Mobile.append(x[20])
    CompanyPhone.append(x[21])
    Territory.append(x[22])
    LeadSource.append(x[23])
    TeamSource.append(x[24])
    CampaignID.append(x[25])
    UploadedCol.append(x[26])
    Email2.append(x[27])
    EmailSource.append(x[28])
    PhoneSource.append(x[29])

Status = 'Backlog'

i = 0
j = range(0,len(BDR))
p = 0
print(len(UploadedCol))

for i in j:
    if (Validated[i] == "TRUE"):
        if (Company[i]!="" and LastName[i]!="" and Title[i]!="" and (Phone[i]!="" or Mobile[i]!="" or CompanyPhone[i]!="" or Email[i]!="" or LeadLinkedin[i]!="") and LeadSource[i]!="" and Country[i]!="" and Region[i]!="" and Vertical[i]!="" and Subvertical[i]!="" and BDRID[i]!=""):  #Check that all fields are filled properly
            try:
                if (UploadedCol[i] == None or UploadedCol[i] == "No"):
                    if (Structure[i] == 'General'):
                        Structure[i] = ''
                    FirstName[i] = FirstName[i].replace("'","").replace('"','').replace('\\','')
                    LastName[i] = LastName[i].replace("'","").replace('"','').replace('\\','')
                    Email[i] = Email[i].replace("'","").replace('"','').replace('\\','')
                    Email2[i] = Email2[i].replace("'","").replace('"','').replace('\\','')
                    Company[i] = Company[i].replace("'","").replace('"','').replace('\\','')
                    QC_response = sf.query("SELECT Id FROM Qualified_company__c WHERE Name = '" + Company[i] +"'")   #Does QC exist ?
                    if (len(QC_response['records']) == 0):  #If QC does not exist then create one
                        qc = sf.Qualified_company__c.create({'Name': Company[i], 'Status__c':'Backlog', 'Country_of_HQ__c': Country[i], 'Region__c': Region[i], 'BDR__c': BDRID[i], 'OwnerId': BDRID[i], 'Vertical__c': Vertical[i], 'Subvertical__c': Subvertical[i], 'Company_Structure__c': Structure[i], 'Website__c': Website[i], 'LinkedIn_URL__c': CompanyLinkedin[i], 'Territory_Plan_Week__c': Territory[i]})
                        QCID = qc['id']
                        lead_response = sf.query("SELECT Id FROM Lead WHERE LastName = '" + str(LastName[i]) + "' AND Email = '" + str(Email[i]) + "'")
                        if (len(lead_response['records']) == 0): #If lead does not exist, create one
                            lead = sf.Lead.create({'Company': Company[i], 'FirstName': FirstName[i], 'LastName':LastName[i], 'Title': Title[i], 'Phone': Phone[i], 'MobilePhone': Mobile[i], 'Other_phone__c': CompanyPhone[i], 'Website': Website[i], 'Status': Status, 'Team_Source__c': TeamSource[i], 'LeadSource': LeadSource[i], 'Country_of_HQ__c': Country[i], 'Region__c': Region[i], 'BDR__c': BDRID[i], 'KAM__c': '', 'OwnerId': BDRID[i], 'Vertical__c': Vertical[i], 'Subvertical__c': Subvertical[i], 'Company_Structure__c': Structure[i], 'Email': Email[i], 'Email_2__c': Email2[i], 'LinkedIn_Link__c': LeadLinkedin[i], 'Qualified_Company__c': QCID, 'Email_Source__c': EmailSource[i], 'Phone_Source__c': PhoneSource[i]})
                            sf.CampaignMember.create({'CampaignId': CampaignID[i],'LeadId':lead['id'],'Status':'Sent'})
                            Error = ''
                            Uploaded = 'Yes'
                            leadId = lead['id']
                            QCID = QCID
                            mycursor = mydb.cursor()
                            sql = "UPDATE MassUpload SET QCID = '" + QCID + "', Uploaded = '" + Uploaded + "', LeadID = '" + leadId + "', FailReason = '" + Error + "' WHERE ID = " + str(ID[i])
                            mycursor.execute(sql)
                            mydb.commit()

                        else: #If lead exists then return message to Sheet
                            info = sf.Lead.get(lead_response['records'][0]['Id'])
                            existingOwner = info['OwnerId']
                            existingLostReason = info['Status']
                            if existingOwner == '00G3W000000pbaBUAQ' or existingLostReason == 'Lost':
                                data = {'Status': 'Backlog', 'OwnerId': BDRID[i], 'BDR__c': BDRID[i]}
                                sf.Lead.update(lead_response['records'][0]['Id'], data)
                                Error = 'Updated'
                                Uploaded = 'Yes'
                                leadId = lead_response['records'][0]['Id']
                                QCID = QCID
                                mycursor = mydb.cursor()
                                sql = "UPDATE MassUpload SET QCID = '" + QCID + "', Uploaded = '" + Uploaded + "', LeadID = '" + leadId + "', FailReason = '" + Error + "' WHERE ID = " + str(ID[i])
                                mycursor.execute(sql)
                                mydb.commit()
                            else:
                                Error = 'Already on SF'
                                Uploaded = 'No'
                                leadId = lead_response['records'][0]['Id']
                                QCID = QCID
                                mycursor = mydb.cursor()
                                sql = "UPDATE MassUpload SET QCID = '" + QCID + "', Uploaded = '" + Uploaded + "', LeadID = '" + leadId + "', FailReason = '" + Error + "' WHERE ID = " + str(ID[i])
                                mycursor.execute(sql)
                                mydb.commit()

                    else: #If QC already exists, grab QC ID
                        QCID = QC_response['records'][0]['Id']
                        lead_response = sf.query("SELECT Id FROM Lead WHERE LastName = '" + str(LastName[i]) + "' AND Email = '" + str(Email[i]) + "'")
                        if (len(lead_response['records']) == 0): #If lead does not exist, create one
                            lead = sf.Lead.create({'Company': Company[i], 'FirstName': FirstName[i], 'LastName':LastName[i], 'Title': Title[i], 'Phone': Phone[i], 'MobilePhone': Mobile[i], 'Other_phone__c': CompanyPhone[i], 'Website': Website[i], 'Status': Status, 'Team_Source__c': TeamSource[i], 'LeadSource': LeadSource[i], 'Country_of_HQ__c': Country[i], 'Region__c': Region[i], 'BDR__c': BDRID[i], 'OwnerId': BDRID[i], 'Vertical__c': Vertical[i], 'Subvertical__c': Subvertical[i], 'Company_Structure__c': Structure[i], 'Email': Email[i], 'Email_2__c': Email2[i], 'Qualified_Company__c': QCID, 'Email_Source__c': EmailSource[i], 'Phone_Source__c': PhoneSource[i]})
                            sf.CampaignMember.create({'CampaignId': CampaignID[i],'LeadId':lead['id'],'Status':'Sent'})
                            Error = ''
                            Uploaded = 'Yes'
                            leadId = lead['id']
                            QCID = QCID
                            mycursor = mydb.cursor()
                            sql = "UPDATE MassUpload SET QCID = '" + QCID + "', Uploaded = '" + Uploaded + "', LeadID = '" + leadId + "', FailReason = '" + Error + "' WHERE ID = " + str(ID[i])
                            mycursor.execute(sql)
                            mydb.commit()

                        else: #If lead exists then return message to Sheet
                            info = sf.Lead.get(lead_response['records'][0]['Id'])
                            existingOwner = info['OwnerId']
                            existingLostReason = info['Status']
                            if existingOwner == '00G3W000000pbaBUAQ' or existingLostReason == 'Lost':
                                data = {'Status': 'Backlog', 'OwnerId': BDRID[i], 'BDR__c': BDRID[i]}
                                sf.Lead.update(lead_response['records'][0]['Id'], data)
                                Error = 'Updated'
                                Uploaded = 'Yes'
                                leadId = lead_response['records'][0]['Id']
                                QCID = QCID
                                mycursor = mydb.cursor()
                                sql = "UPDATE MassUpload SET QCID = '" + QCID + "', Uploaded = '" + Uploaded + "', LeadID = '" + leadId + "', FailReason = '" + Error + "' WHERE ID = " + str(ID[i])
                                mycursor.execute(sql)
                                mydb.commit()
                            else:
                                Error = 'Already on SF'
                                Uploaded = 'No'
                                leadId = lead_response['records'][0]['Id']
                                QCID = QCID
                                mycursor = mydb.cursor()
                                sql = "UPDATE MassUpload SET QCID = '" + QCID + "', Uploaded = '" + Uploaded + "', LeadID = '" + leadId + "', FailReason = '" + Error + "' WHERE ID = " + str(ID[i])
                                mycursor.execute(sql)
                                mydb.commit()

            except Exception as e:
                try:
                    Error = str(e)
                    start = Error.find("'errorCode': '") + len("'errorCode': '")
                    end = Error.find("', 'fields'")
                    Error = Error[start:end]
                    Uploaded = 'No'
                    leadId = ''
                    QCID = ''
                    mycursor = mydb.cursor()
                    Error.replace("'"," ").replace('"',' ').replace('\\',' ')
                    sql = "UPDATE MassUpload SET QCID = '" + QCID + "', Uploaded = '" + Uploaded + "', LeadID = '" + leadId + "', FailReason = '" + Error + "' WHERE ID = " + str(ID[i])
                    mycursor.execute(sql)
                    mydb.commit()
                except:
                    Error = 'Unknown'
                    Uploaded = 'No'
                    leadId = ''
                    QCID = ''
                    mycursor = mydb.cursor()
                    sql = "UPDATE MassUpload SET QCID = '" + QCID + "', Uploaded = '" + Uploaded + "', LeadID = '" + leadId + "', FailReason = '" + Error + "' WHERE ID = " + str(ID[i])
                    mycursor.execute(sql)
                    mydb.commit()
        else:
            Error = 'Empty Fields'
            Uploaded = 'No'
            leadId = ''
            QCID = ''
            mycursor = mydb.cursor()
            sql = "UPDATE MassUpload SET QCID = '" + QCID + "', Uploaded = '" + Uploaded + "', LeadID = '" + leadId + "', FailReason = '" + Error + "' WHERE ID = " + str(ID[i])
            mycursor.execute(sql)
            mydb.commit()

    else:
        Error = 'Not Validated'
        Uploaded = 'No'
        leadId = ''
        QCID = ''
        mycursor = mydb.cursor()
        sql = "UPDATE MassUpload SET QCID = '" + QCID + "', Uploaded = '" + Uploaded + "', LeadID = '" + leadId + "', FailReason = '" + Error + "' WHERE ID = " + str(ID[i])
        mycursor.execute(sql)
        mydb.commit()
