from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
import datetime
import boto3
from botocore.errorfactory import ClientError
from selenium import webdriver
from utils.dbutils import *
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import Select
import zipfile
import numpy as np
import shutil
import os

##test function to list files
def list_files(startpath):
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f))
           
#if the options method doesn't work, then use the below to set downloads
 
# profile = webdriver.FirefoxProfile()
# profile.set_preference("browser.download.folderList", 2)
# profile.set_preference("browser.download.manager.showWhenStarting", False)
# profile.set_preference("browser.download.dir", '/opt/downloads')
# profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/x-gzip")


#allow downloads without a popup
options = Options()
options.headless = True
options.set_preference("browser.download.folderList",2)
options.set_preference("browser.download.manager.showWhenStarting", False)
options.set_preference("browser.download.dir","/opt/downloads")
options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream,application/vnd.ms-excel,pplication/x-gzip")
driver=webdriver.Firefox(options=options, executable_path='/opt/geckodriver')

conn = create_server_connection(os.environ(["aurora_endpoint"]), get_secret()['username'], get_secret()['password'])


##keep track of what item you're using
cnt = 0
#cnt = 59
##do everything inside a loop; need to refresh page each time
while True:

    driver.get('https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx')
    
    
    ##find the list box and select "single period" for reporting cycle
    dl_type = Select(driver.find_element_by_id('ListBox1'))
    dl_type.select_by_value('ReportingSeriesSinglePeriod')
    
    #sleep for page to reload
    time.sleep(6)
    
    periods = Select(driver.find_element_by_id('DatesDropDownList'))
    
    
    ##get the filing dates in the table and the filing dates that have been scraped
    filing_dates = read_query(conn, 'select filing_date from ffiec_raw.forms_scraped')
    filing_dates_scraped = read_query(conn, 'select filing_date from ffiec_raw.forms_scraped where RCK = 1')
    filing_dates_scraped = [x[0].strftime("%m/%d/%Y") for x in filing_dates_scraped]
    #print(filing_dates)
    
    dates = [period.text for period in periods.options]
    
    
    ##insert any dates into the forms to scrape table
    dates_insert = ['(STR_TO_DATE("' + date + '", "%m/%d/%Y"))' for date in dates if date not in [x[0].strftime("%m/%d/%Y") for x in filing_dates]]
    dates_insert.reverse()
    #print(dates_insert)
    print(len(dates_insert))
    
    #insert any dates that aren't present in the forms to scrape table
    if len(dates_insert) > 0:
        query = 'insert into ffiec_raw.forms_scraped (filing_date) values {};'.format(', '.join(dates_insert))
        #print(query)
    
        execute_query(conn, query)
        
        
    #break the while loop because they are bad
    if cnt == len(periods.options):
        #if cnt == 60:
        break
    
    #period = periods.options[cnt]
    ##get the date early because otherwise it will time out
    str1 = periods.options[cnt].text
    
    ##if the date hasn't been scraped yet, then download and scape the file
    if str1 not in filing_dates_scraped:
    
        ##select the appropriate date period and download the file
        periods.select_by_index(cnt)
        
        time.sleep(3)
        
        submit_button = driver.find_element_by_id('Download_0')
        submit_button.click()
        
        ##wait for the zip to finish donloading
        time.sleep(20)
        
        #list_files('/opt/downloads/')
        
        zips = []
        
        ##rename the zip file to not have spaces
        for root, dirs, files in os.walk('/opt/downloads/'):
            count = 0
            for f in files:
                count = count + 1
                print(f)
                os.rename('/opt/downloads/' + f, '/opt/downloads/download' + str(count) + '.zip')
        
        #print(zips)
        #list_files('/opt/downloads/')
        
        ##unzip the file
        with zipfile.ZipFile('/opt/downloads/download1.zip', 'r') as zip_ref:
            zip_ref.extractall('/opt/downloads/unzipped/')
        #list_files('/opt/downloads/')
        
        bl = 0
        ##rename the file in question
        for root, dirs, files in os.walk('/opt/downloads/unzipped/'):
            for f in files:
                if ' Schedule RCK ' in f:
                    bl = 1
                    os.rename('/opt/downloads/unzipped/' + f, '/opt/downloads/unzipped/schedrc.txt')
        
        #list_files('/opt/downloads/unzipped/')
        
        ##some years don't have the file we care about
        if bl == 1:
            ##read the data, skip the second row
            data = pd.read_csv('/opt/downloads/unzipped/schedrc.txt', sep="\t", skiprows = [1])
            
            #print(data.head(5))
            
            ##get rid of any extra coumns (sometimes someone adds an extra tab)
            for col in data.columns:
                if 'Unnamed' in col:
                    data = data.drop(columns = [col])
            
            ##put null for sql
            data = data.replace(np.nan, 'NULL', regex=True)
            
            #convert date to sql
            data['date'] = 'STR_TO_DATE("{}", "%m/%d/%Y")'.format(str1)
            
            #print(data.head(5))
            
            #print(pd.io.sql.get_schema(data, 'data'))
            
            ##get the appropriate filing_id
            query = 'select filing_id from ffiec_raw.forms_scraped where filing_date = STR_TO_DATE("{}", "%m/%d/%Y")'.format(str1)
                #print(query)
            
            out = read_query(conn, query)
            
            #print(query)
            #print(out)
            
            data['filing_id'] = out[0][0]
            
            
            #print(data.head(5))
            
            insert_query = 'insert into ffiec_raw.rck_raw ('
            for col in data.columns:
                insert_query = insert_query + col + ','
                
            insert_query = insert_query[:-1] + ') VALUES ('
            
            try:
                for i,row in data.iterrows():
                    sql = insert_query + "%s,"*(len(row)-1) + "%s)" #% tuple(row)
            
                    sql = sql % tuple(row)
                    #print(sql)
                    #print(sql)
                    ##this query actually produces an error so the whole thing rolls back
                    execute_query_safe(conn, sql)
            
                execute_query(conn, 'update ffiec_raw.forms_scraped set RCK = 1 where filing_id = ' + str(out[0][0]))
            except Error as err:
                print(f"Error: '{err}'")
                cleanup_query = 'delete from ffiec_raw.rck_raw where filing_id = ' + str(out[0][0])
                #pass
                execute_query(conn, cleanup_query)
                
            #remove the files
        os.remove('/opt/downloads/download1.zip')
        shutil.rmtree('/opt/downloads/unzipped/')
    else:
        print('SKIPPED')
    
    print('CNT: ' + str(cnt))
    
    cnt = cnt + 1
