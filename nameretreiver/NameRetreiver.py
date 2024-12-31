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

def list_files(startpath):
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f))
            
# profile = webdriver.FirefoxProfile()
# profile.set_preference("browser.download.folderList", 2)
# profile.set_preference("browser.download.manager.showWhenStarting", False)
# profile.set_preference("browser.download.dir", '/opt/downloads')
# profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/x-gzip")


options = Options()
options.headless = True
options.set_preference("browser.download.folderList",2)
options.set_preference("browser.download.manager.showWhenStarting", False)
options.set_preference("browser.download.dir","/opt/downloads")
options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream,application/vnd.ms-excel,pplication/x-gzip")
driver=webdriver.Firefox(options=options, executable_path='/opt/geckodriver')

cnt = 0

#periods = [period.text for period in periods.options]

# if not cnt:
#     print('{} available reporting periods: {}'.format(
#         len(periods.options),
#         ', '.join([period.text for period in periods.options])
#     ))

conn = create_server_connection(os.environ(["aurora_endpoint"]), get_secret()['username'], get_secret()['password'])

#dates = [period.text for period in periods.options]
while True:
    
    driver.get('https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx')

    dl_type = Select(driver.find_element_by_id('ListBox1'))
    dl_type.select_by_value('ReportingSeriesSinglePeriod')

    time.sleep(6)

    periods = Select(driver.find_element_by_id('DatesDropDownList'))
    
    if cnt == len(periods.options):
        break
    
    #period = periods.options[cnt]
    
    str1 = periods.options[cnt].text
    
    periods.select_by_index(cnt)
    
    time.sleep(3)
    
    submit_button = driver.find_element_by_id('Download_0')
    submit_button.click()
    
    time.sleep(20)
    
    list_files('/opt/downloads/')
    
    zips = []
    for root, dirs, files in os.walk('/opt/downloads/'):
        count = 0
        for f in files:
            count = count + 1
            print(f)
            os.rename('/opt/downloads/' + f, '/opt/downloads/download' + str(count) + '.zip')
    
    print(zips)
    list_files('/opt/downloads/')
    
    with zipfile.ZipFile('/opt/downloads/download1.zip', 'r') as zip_ref:
        zip_ref.extractall('/opt/downloads/unzipped/')
    list_files('/opt/downloads/')
    
    bl = 0
    
    for root, dirs, files in os.walk('/opt/downloads/unzipped/'):
        for f in files:
            if ' Schedule RCK ' in f:
                bl = 1
                os.rename('/opt/downloads/unzipped/' + f, '/opt/downloads/unzipped/schedrc.txt')
    
    list_files('/opt/downloads/unzipped/')
    
    if bl == 1:
        data = pd.read_csv('/opt/downloads/unzipped/schedrc.txt', sep="\t", nrows = 1)
        
        print(data.head(5))
        
        for col in data.columns:
            if 'Unnamed' in col:
                data = data.drop(columns = [col])
                
        print(data)
        
        
        
        writetable = pd.DataFrame({'field_name': data.columns, 'field_label': data.values.tolist()[0]})
        writetable['date'] = 'STR_TO_DATE("{}", "%m/%d/%Y")'.format(str1)
        
        insert_query = 'insert into ffiec_raw.rck_fields (field_name, field_label, date) VALUES ('
        
        for i,row in writetable.iterrows():
            sql = insert_query + '"%s", "%s", %s)' #% tuple(row)
    
            sql = sql % tuple(row)
            print(sql)
            #print(sql)
            execute_query(conn, sql)
        
        
    os.remove('/opt/downloads/download1.zip')
    
    shutil.rmtree('/opt/downloads/unzipped/')
    
    cnt = cnt + 1
    
    print('CNT' + str(cnt))

