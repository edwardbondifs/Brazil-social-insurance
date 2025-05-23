#!/usr/bin/env python
# coding: utf-8

# **Next method**

# In[1]:


import sys
print(sys.executable)


# ***Pyautogui***

# In[2]:


import pyautogui
import os
import time
import subprocess
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
import psutil
from selenium.webdriver.support.ui import Select
import matplotlib.pyplot as plt


# In[3]:


os.chdir("C:/Users/edward_b/OneDrive - Institute for Fiscal Studies/Work/Brazil social insurance")


# In[4]:


# Data storage
data = []

# Function to kill Chrome processes
def kill_chrome():
    """Kill all Chrome processes."""
    for proc in psutil.process_iter(['pid', 'name']):
        if 'chrome' in proc.info['name'].lower():
            try:
                proc.kill()
            except psutil.NoSuchProcess:
                pass

# Scraper function
def scrape_data(cnpj,year):
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    table = soup.find('table', class_='table table-hover table-condensed emissao is-detailed')
    if not table:
        print(f"No table found for {cnpj}")
        data.append({'cnpj': cnpj,
                      'periodo': year,
                      'data_found': False})
        return
    rows = table.find('tbody').find_all('tr', class_='pa')
    for row in rows:
        cells = row.find_all('td')
        if len(cells) < 10:
            continue
        data.append({
            'cnpj': cnpj,
            'periodo': cells[1].get_text(strip=True),
            'apurado': cells[2].get_text(strip=True),
            'INSS': cells[3].get_text(strip=True),
            'principal': cells[4].get_text(strip=True),
            'multa': cells[5].get_text(strip=True),
            'juros': cells[6].get_text(strip=True),
            'total': cells[7].get_text(strip=True),
            'vencimento': cells[8].get_text(strip=True),
            'acolhimento': cells[9].get_text(strip=True),
            'data_found': True
        })


# **Import master**

# In[8]:


path = "raw/CNPJ numbers"
cnpj_master = pd.read_csv(f'{path}/simples.csv', sep=',', encoding='utf-8')
cnpj_master = cnpj_master[['cnpj_basico','opcao_mei']]


# In[9]:


# find length of cnpj
cnpj_master["length cnpj_basico"] = cnpj_master["cnpj_basico"].astype(str).str.len()


# In[10]:


pd.crosstab(cnpj_master["length cnpj_basico"], cnpj_master["opcao_mei"], margins=True, margins_name="Total")


# In[11]:


cnpj_master["cnpj_basico"] = cnpj_master["cnpj_basico"].astype(str)


# In[12]:


cnpj_master = cnpj_master[cnpj_master['opcao_mei'] == 1]
cnpj_master.drop(columns=['length cnpj_basico'], inplace=True)


# **Import example full file**

# In[13]:


cnpj_test = pd.read_csv(f'{path}/establishmentsAC.csv', sep=',', encoding='utf-8')
cnpj_test = cnpj_test[['cnpj']]
cnpj_test["cnpj_basico"] = cnpj_test["cnpj"].astype(str).str[:8]


# In[14]:


cnpj_merged = pd.merge(cnpj_master, cnpj_test, left_on='cnpj_basico', right_on='cnpj_basico', how='inner')


# In[15]:


cnpj_merged["cnpj"] = cnpj_merged["cnpj"].astype(str)
cnpj_merged["cnpj"].str.len().hist()


# In[16]:


cnpj_merged = cnpj_merged[cnpj_merged['cnpj'].str.len() == 14]
cnpj_merged = cnpj_merged.drop_duplicates(subset=['cnpj'], keep='first')


# In[17]:


cnpj_merged.to_csv('MEI_numbers.csv', sep=',', encoding='utf-8', index=False)


# In[5]:


cnpj_merged = pd.read_csv('MEI_numbers.csv', sep=',', encoding='utf-8')


# In[8]:


cnpj_merged


# In[6]:


# get random sample of 10
cnpj_merged = cnpj_merged.sample(n=1, random_state=10)
#convert cnpj's to a list
cnpj_list = cnpj_merged['cnpj'].tolist()


# In[7]:


cnpj_merged.shape # 13,894 obs


# **Set up scraper**

# In[8]:


cnpj_list = [str(i) for i in cnpj_list]
cnpj_list


# In[9]:


import shutil
import os

chrome_profile_path = "C:/Temp/ChromeDebug"
if os.path.exists(chrome_profile_path):
    shutil.rmtree(chrome_profile_path)  # delete the profile folder


# In[16]:


pyautogui.position()


# In[17]:


# ---- Define url and cnpj list ----
url = "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/Identificacao"

for cnpj in cnpj_list:
    print("Processing CNPJ:", cnpj)

    try:

        # ---- Step 1: Start Chrome in remote debug mode ----
        subprocess.Popen([
            r"C:/Program Files/Google/Chrome/Application/chrome.exe",
            "--remote-debugging-port=9222",
            "--user-data-dir=" + chrome_profile_path,
            "--start-maximized",  # or "--start-fullscreen"
            "--disable-popup-blocking",  # optional, disable for debugging only
            "--disable-extensions",
            "--no-first-run",
            "--no-default-browser-check"
        ])
        time.sleep(2)  # Give Chrome time to launch

        # ---- Step 2: Use pyautogui to interact with the site ----
        pyautogui.hotkey('ctrl', 'l')
        pyautogui.typewrite(url, interval=0.01)
        pyautogui.press('enter')   
        time.sleep(2)

        pyautogui.moveTo(x=1037, y=387, duration=1)
        pyautogui.click()
        pyautogui.typewrite(cnpj, interval=0.1)

        pyautogui.moveTo(x=1043, y=514, duration=1)
        pyautogui.click()
        time.sleep(2)

        options = Options()
        options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
        driver = webdriver.Chrome(options=options)
        wait = WebDriverWait(driver, 3)

        driver.get("https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/emissao")
        time.sleep(1.5)

        # First try: Bootstrap-styled dropdown
        try:
            dropdown_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-id="anoCalendarioSelect"]')))
            dropdown_button.click()
            time.sleep(1)

            year_elements = wait.until(EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, ".dropdown-menu.inner li a span.text")
            ))
            enabled_years = [el.text.strip() for el in year_elements if el.text.strip()]
            # remove elements from the list that contain "Não optante"
            enabled_years = [year for year in enabled_years if "Não optante" not in year]

            # Raise an exception if no enabled years are found
            if not enabled_years:
                raise ValueError("No enabled years found in the dropdown menu.")

            print("Bootstrap dropdown enabled years for CNPJ ", cnpj , ":", enabled_years)
            use_bootstrap = True


        except Exception as e:
            print("Bootstrap dropdown failed, falling back to native <select> method.")
            # Try native <select>
            select_element = wait.until(EC.presence_of_element_located((By.ID, "anoCalendarioSelect")))
            dropdown = Select(select_element)
            enabled_years = [o.text.strip() for o in dropdown.options if o.text.strip()]
            enabled_years = [year for year in enabled_years if "Não optante" not in year]
            print("Native <select> enabled years for CNPJ ", cnpj,":", enabled_years)
            use_bootstrap = False

        print("scraping years", enabled_years)
        enabled_years = [str(max(enabled_years))]
        enabled_years.insert(0, "2010")  #add a year to the start of the list

        for year in enabled_years:
            try:
                if use_bootstrap:
                    dropdown_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'button[data-id="anoCalendarioSelect"]')))
                    driver.execute_script("arguments[0].click();", dropdown_button)
                    time.sleep(1.5)  # allow dropdown to render

                    print("Clicking on year:", year)
                    year_option = wait.until(EC.element_to_be_clickable(
                        (By.XPATH, f"//span[@class='text' and normalize-space(text())='{year}']")
                    ))
                    time.sleep(2)
                    driver.execute_script("arguments[0].click();", year_option)
                    #ActionChains(driver).move_to_element(year_option).click().perform()
                    print(f"Selected (Bootstrap) year: {year}")
                else:
                    dropdown = Select(driver.find_element(By.ID, "anoCalendarioSelect"))
                    dropdown.select_by_visible_text(year)
                    print(f"Selected (native) year: {year}")

                ok_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
                ok_button.click()
                time.sleep(2)

                scrape_data(cnpj,year)

                driver.back()
                time.sleep(2)

            except Exception as e:
                print(f"Error with year {year}:", e)

    except Exception as outer_error:
        print(f"Fatal error with CNPJ {cnpj}:", outer_error)

    finally:
        try:
            driver.quit()
        except:
            pass
        kill_chrome()
        print("Finished CNPJ:", cnpj)
        time.sleep(2)


# In[18]:


data


# In[19]:


pd.DataFrame(data).to_csv('MEI_data.csv', sep=',', encoding='utf-8', index=False)

