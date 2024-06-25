from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.common.action_chains import ActionChains

# Set up Chrome options (optional)
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run headless if you don't want to open the browser window

# Specify the path to chromedriver
chromedriver_path = '/path/to/chromedriver'  # Update this path

# Set up the WebDriver (e.g., Chrome)
service = Service(chromedriver_path)
driver = webdriver.Chrome(service=service, options=chrome_options)

# Navigate to the web page
driver.get('http://example.com')  # Replace with the actual URL of the web page

# Define the XPaths
dropdown_xpath = '//input[@class="mantine-Input-input mantine-Select-input mantine-19iv9wd"]'
option_xpath_template = '//div[@class="mantine-1qdgdgy mantine-Select-item" and text()="{}"]'  # Template for selecting option by text

from selenium.webdriver.common.action_chains import ActionChains


# Function to click the dropdown and select an option using ActionChains
def select_dropdown_option_with_action_chains(driver, dropdown_xpath, option_text, retries=5):
    for i in range(retries):
        try:
            # Wait for the dropdown input to be present and click it using ActionChains
            dropdown_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, dropdown_xpath))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", dropdown_element)
            actions = ActionChains(driver)
            actions.move_to_element(dropdown_element).click().perform()

            # Wait for the option to be present and click it using ActionChains
            option_xpath = option_xpath_template.format(option_text)
            option_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, option_xpath))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", option_element)
            actions.move_to_element(option_element).click().perform()

            print(f"Option '{option_text}' selected successfully")
            return

        except (StaleElementReferenceException, NoSuchElementException, ElementClickInterceptedException) as e:
            if i < retries - 1:
                print(f"Retry {i + 1}/{retries} due to exception: {e}")
                continue
            else:
                raise
        except Exception as e:
            raise e


# Select the option '100' from the dropdown using ActionChains
try:
    select_dropdown_option_with_action_chains(driver, dropdown_xpath, "100")
except Exception as e:
    print(f"An error occurred: {e}")

# Close the browser
driver.quit()


