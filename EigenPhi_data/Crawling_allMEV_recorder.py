from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import json


def FetchEigenPhi():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    url = 'https://eigenphi.io/mev/bsc/txr'
    driver.get(url)

    time.sleep(15)

    # Specify the file path
    file_path = '/Users/andyma/Desktop/Python /Tokkalabs_WorkSpace/EigenPhi_data/Data_crawled/data_at_'
    time_forSave = 60*60*6 # save the data every 6 hours
    record={}
    # i=0
    time_acc = 0
    while True:
        # if i==2:
        #     break
        try:
            time_start=time.time()
            section = driver.find_element(By.CLASS_NAME, 'mantine-vnv9e5')
            print("Found section")
        except:
            print('find problem in getting section wait jump into the next round')
            time.sleep(3)
            continue

        table = section.find_element(By.CLASS_NAME, 'mantine-Table-root')
        print("Found table")


        all_data=[]

        try:
            rows = table.find_elements(By.TAG_NAME, 'tr')
        except:
            break

        for row in rows[1:]:
            try:
                cols = row.find_elements(By.TAG_NAME, 'td')
            except:
                break

            row_data = []
            for col in cols:
                try:
                    link = col.find_element(By.TAG_NAME, 'a') if col.find_elements(By.TAG_NAME, 'a') else None
                    if link:
                        row_data.append(link.text)
                        row_data.append(link.get_attribute('href'))
                    else:
                        row_data.append(col.text)
                except:
                    continue
            all_data.append(row_data)

        print("Page data extracted")
        record[time_start]=all_data
        time_used = time.time() - time_start
        time_acc += time_used
        # i += 1
        if time_acc >= time_forSave:
            time_acc = 0
            with open(file_path+str(time_start)+'.json', 'w') as json_file:
                json.dump(record, json_file, indent=4)
            print(f"Record saved at {time_start}")
            record = {} # reset record

    driver.quit()
    return record

if __name__ == '__main__':
    data=FetchEigenPhi()



