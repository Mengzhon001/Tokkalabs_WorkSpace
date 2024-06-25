from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd

options = webdriver.ChromeOptions()
options.add_argument('--headless')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

url = 'https://eigenphi.io/mev/bsc/txr'
driver.get(url)

time.sleep(15)


all_data=[]

i = 0

while True:
    if i > 2:
        break
    time_start = time.time()
    print(f"Extracting data at time {time_start}")
    ######################################## Read the table

    section = driver.find_element(By.CLASS_NAME, 'mantine-vnv9e5')
    print("Found section")

    table = section.find_element(By.CLASS_NAME, 'mantine-Table-root')
    print("Found table")

    # headers = [header.text for header in table.find_elements(By.TAG_NAME, 'th')]
    # print("Headers:", headers)
    headers = ['Time', 'Tx_url', 'Block_url', 'Tx', 'Block', 'Token_url', 'Token', 'From', 'Contract', 'Contract_url', 'Profit', 'Cost', 'Revenue', 'Type']
    # headers.insert(1,'Tx_url')
    # headers.insert(3,'Block_url')
    # headers.insert(6,'Token_url')
    # headers.insert(10,'Contract_url')
    # print(headers)

    rows = table.find_elements(By.TAG_NAME, 'tr')
    table_data = []
    for row in rows[1:]:
        cols = row.find_elements(By.TAG_NAME, 'td')
        row_data = []
        for col in cols:
            link = col.find_element(By.TAG_NAME, 'a') if col.find_elements(By.TAG_NAME, 'a') else None
            if link:
                row_data.append(link.text)
                row_data.append(link.get_attribute('href'))
            else:
                row_data.append(col.text)
        row_data[0] = time_start
        table_data.append(row_data)

    print("Table data extracted")
    all_data.append(table_data)
    time_lapse=time.time()-time_start
    if time_lapse < 3:
        time.sleep(3-time_lapse)

    i += 1
    pass

df = pd.DataFrame(table_data, columns=headers)
df.to_csv('table_data.csv', index=False)

# Close the browser
driver.quit()




