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

url = 'https://eigenphi.io/mev/bsc/sandwich'
driver.get(url)

time.sleep(15)

try:
    ######################################## Choose date
    pick_date_button = driver.find_element(By.XPATH, "//input[@placeholder='Pick date']")
    print("Found date picker button")
    pick_date_button.click()

    wait = WebDriverWait(driver, 30)
    date_picker_dropdown = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, 'mantine-DatePicker-dropdown')))
    print("Date picker dropdown visible")

    date_button = date_picker_dropdown.find_element(By.XPATH, "//button[@class='mantine-4t7fl6 mantine-DatePicker-day' and text()='14']")
    print("Found date button")
    date_button.click()

    time.sleep(15)

    ######################################## Read the table across multiple pages

    all_data = []
    headers = None

    i = 0

    while True:
        if i > 10:
            break
        print(f"Extracting data from page {i + 1}")
        section = driver.find_element(By.CLASS_NAME, 'mantine-vnv9e5')
        print("Found section")

        table = section.find_element(By.CLASS_NAME, 'mantine-Table-root')
        print("Found table")

        if headers is None:
            headers = [header.text for header in table.find_elements(By.TAG_NAME, 'th')]
            print("Headers:", headers)
            headers.insert(2, 'Transaction Hash URL')

        rows = table.find_elements(By.TAG_NAME, 'tr')
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
            all_data.append(row_data)
        print("Page data extracted")
        i += 1

        try:
            next_button = driver.find_element(By.XPATH, "/html/body/div[1]/div/div/main/div[1]/div/div[4]/section/table/tfoot/tr/td/div/div/div/div[3]/button[2]")
            driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
            time.sleep(1)  # Wait for scrolling to complete
            driver.execute_script("arguments[0].click();", next_button)
            time.sleep(5)  # Wait for the page to load
        except Exception as e:
            print(f"No more pages or an error occurred: {e}")
            break

    df = pd.DataFrame(all_data, columns=headers)
    df.to_csv('table_data.csv', index=False)
    print("Table data saved to table_data.csv")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    driver.quit()
