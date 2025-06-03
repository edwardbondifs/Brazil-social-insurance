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
import logging
from logging.handlers import RotatingFileHandler
import random
import requests
import re
import fitz  # PyMuPDF

# --- PDF and File Utilities ---
def extract_cpf(file):
    try: 
        # Load the PDF
        doc = fitz.open(file)

        # Extract text from all pages
        text = ""
        for page in doc:
            text += page.get_text() 


        cnpj = text.split("\n")[2]
        cpf_match = re.search(r"CPF[:\s]*([\d.-]+)", text)

        cnpj = re.sub(r'\D', '', cnpj)
        cpf = re.sub(r'\D', '', cpf_match.group(1)) if cpf_match else None

        return cnpj, cpf
    except Exception as e: 
        print(f"Error extracting CPF/CNPJ from PDF: {e}")
        return None, None 

 # Obtain PDF
def obtain_pdf(driver, wait, period):
    try:
        # Select December checkbox
        checkbox = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, f'input[name="pa"][value$="{period}"]')
        ))
        driver.execute_script("arguments[0].click();", checkbox)
        print(f"✔️ Checkbox selected for {period}")

        time.sleep(1)

        # Click Apurar / DAS button
        das_button = wait.until(EC.element_to_be_clickable((By.ID, "btnEmitirDas")))
        driver.execute_script("arguments[0].click();", das_button)
        print("✔️ Apurar / DAS button clicked for 2025")

        time.sleep(2)

        # Click Imprimir/Visualizar PDF
        try:
            pdf_link = wait.until(EC.element_to_be_clickable(
                (By.XPATH, '//a[contains(@href, "/pgmei.app/emissao/imprimir")]')
            ))
            driver.execute_script("arguments[0].click();", pdf_link)
            print("✔️ PDF print view opened")
        except Exception as e:
            print(f"❌ Failed to click PDF print button: {e}")
    except Exception as e:
        print(f"❌ Error obtaining PDF: {e}")       

# --- Browser/Process Utilities ---
def kill_chrome():
    """Kill all Chrome processes."""
    for proc in psutil.process_iter(['pid', 'name']):
        if 'chrome' in proc.info['name'].lower():
            try:
                proc.kill()
            except psutil.NoSuchProcess:
                pass
def autogui_open_page(chrome_profile_path, url, cnpj):
    try:
        # ---- Step 1: Start Chrome in remote debug mode ----
        subprocess.Popen([
            r"C:/Program Files/Google/Chrome/Application/chrome.exe",
            #f"--proxy-server={proxy}",
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

        pyautogui.moveTo(x=1027, y=377, duration=1) # laptop x=722, y=391  deksptop : x=1027, y=377
        pyautogui.click()
        pyautogui.typewrite(cnpj, interval=0.1)

        pyautogui.moveTo(x=1027, y=500, duration=1) # Laptop x=722, y=514 desktop: x=1027, y=500
        pyautogui.click()
        time.sleep(2)
    except Exception as e:
        print(f"Error opening page with pyautogui: {e}")
        return False
def selenium_open_page(url_inside):

    # Selenium setup
    options = Options()
    options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 3)

    driver.get(url_inside)
    time.sleep(1.5)
    return driver, wait

# --- Scraping Utilities ---
def cnpj_check(driver, cnpj):
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    cnpj_check = re.sub(r'\D','',soup.find("li", class_="list-group-item").get_text(strip=True).split("Nome")[0])
    # compare the cnpj found on the page with the one we are looking for
    print(f"cnpj_check: {cnpj_check}")
    print(f"cnpj: {cnpj}")
    if cnpj_check != cnpj:
        raise ValueError(f"CNPJ mismatch: expected {cnpj}, found {cnpj_check}")
def scrape_data(cnpj, year, soup, table):
    # Step 1: Extract column headers (excluding unwanted labels)
    header_rows = table.find('thead').find_all('tr')
    cols = []
    for row in header_rows:
        headers = [th.get_text(strip=True) for th in row.find_all("th") if th.get_text(strip=True)]
        filtered = [h for h in headers if h != "Resumo do DAS a ser gerado"]
        cols.extend(filtered)
    print(f"Extracted headers: {cols}")

    # Check if "Quotas" is in the headers to determine if we need to split rows
    quota_split = "Quotas" in cols
    quota_index = cols.index("Quotas") if quota_split else None #Find index of "Quotas" column if it exists

    # Find index of "INSS" column if it exists
    inss_index = cols.index("Benefício INSS") if "Benefício INSS" in cols else None #Find index of "Benefício INSS" column if it exists

    # Step 2: Find all relevant data rows
    rows = soup.find_all("tr", class_="pa")

    # Step 3: Process data rows with split-row logic
    cleaned_data = []
    i = 0
    while i < len(rows):
        row = rows[i]
        cells = row.find_all("td")
        
        # Check if the row has INSS box ticked
        inss_row = any(
            inp.get("data-benefico-apurado") == "True"
            for inp in row.find_all("input", attrs={"data-benefico-apurado": True})
        )
        
        # Extract visible text from the cells (skipping the first <td> with checkbox)
        cell_texts = [td.get_text(strip=True) for td in cells[1:]]
        cell_texts[inss_index] = "1" if inss_row else "0"
        
        if quota_split: #If we have a table with a quotas column
        
            # Check if each row  has quotas that require a split
            quota_row = any(
                inp.get("data-pa-quota") == "true"
                for inp in row.find_all("input", attrs={"data-pa-quota": True})
            )


            if quota_row:
                # First 4 cells: Período, Apurado, Benefício, Quotas (set to 1)
                base_info = cell_texts[:4]
                base_info[quota_index] = "1" if quota_split else "0"
                payment_data = cell_texts[4:]
                cleaned_data.append(base_info + payment_data)

                # Append next row with same identifying info if exists
                if i + 1 < len(rows):
                    next_row = rows[i + 1]
                    next_cells = next_row.find_all("td")
                    next_texts = [td.get_text(strip=True) for td in next_cells]

                    cleaned_data.append(base_info + next_texts)
                    i += 2
                else:
                    i += 1
            else:
                # Normal row within a quotas table, treat quotas as 0 if not explicitly set
                if len(cell_texts) >= 5:
                    cell_texts[3] = "0"
                cleaned_data.append(cell_texts)
                i += 1

        # Normal table without quotas
        else:    
            cleaned_data.append(cell_texts)
            i += 1

    
    # Step 4: Build DataFrame
    df = pd.DataFrame(cleaned_data, columns=cols)
    df['cnpj'] = cnpj
    df['data_found'] = True

    return df
def debt_collector(soup):
    # Placeholder for debt collector logic.
    # if soup contains ATENÇÃO: Existe(m) débitos(s) enviados(s) para inscrição em dívida ativa.then return True
    attention_text = soup.find(text=re.compile(r"ATENÇÃO: Existe\(m\) débitos\(s\) enviados\(s\) para inscrição em dívida ativa"))
    if attention_text:
        return True
    return False
def outstanding_payment(data):
    mask = data['Total'].astype(str).str.strip().ne("-")
    if mask.any():
        first = mask.idxmax()
        month = first + 1
        return f"{month:02d}"
    return None
def scrape_debt_table(cnpj, soup):
    all_rows = []
    table = soup.find_all("table", class_="table table-bordered table-hover table-condensed")
    for tbl in table:
        # Get the period from the caption
        caption = tbl.find("caption")
        periodo = caption.get_text(strip=True).replace("Período de Apuração (PA): ", "") if caption else None

        # Get all rows in tbody
        for tbody in tbl.find_all("tbody"):
            for tr in tbody.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) == 4:
                    tributo = tds[0].get_text(strip=True)
                    valor = tds[1].get_text(strip=True)
                    ente = tds[2].get_text(strip=True)
                    situacao = tds[3].get_text(strip=True)
                    all_rows.append({
                        "Periodo de Apuracao": periodo,
                        "Tributo": tributo,
                        "Valor": valor,
                        "Ente Federado": ente,
                        "Situacao do Debito": situacao
                    })
    df = pd.DataFrame(all_rows)
    df["cnpj"] = cnpj
    return df
  
# --- Dropdown and Year Selection ---
def get_enabled_years_bootstrap(wait, cnpj):
    """
    Try to get enabled years from a Bootstrap-styled dropdown.
    Returns (enabled_years, use_bootstrap).
    Raises if not found.
    """
    dropdown_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-id="anoCalendarioSelect"]')))
    dropdown_button.click()
    time.sleep(1)

    year_elements = wait.until(EC.presence_of_all_elements_located(
        (By.CSS_SELECTOR, ".dropdown-menu.inner li a span.text")
    ))
    enabled_years = [el.text.strip() for el in year_elements if el.text.strip()]
    enabled_years = [year for year in enabled_years if "Não optante" not in year]

    if not enabled_years:
        raise ValueError("No enabled years found in the dropdown menu.")

    print("Bootstrap dropdown enabled years for CNPJ ", cnpj , ":", enabled_years)
    return enabled_years, True
def get_enabled_years_native(wait, cnpj, driver):
    """
    Get enabled years from a native <select> dropdown.
    Returns (enabled_years, use_bootstrap).
    """
    select_element = wait.until(EC.presence_of_element_located((By.ID, "anoCalendarioSelect")))
    dropdown = Select(select_element)
    enabled_years = [o.text.strip() for o in dropdown.options if o.text.strip()]
    enabled_years = [year for year in enabled_years if "Não optante" not in year]
    print("Native <select> enabled years for CNPJ ", cnpj,":", enabled_years)
    return enabled_years, False
def select_year_bootstrap(wait, driver, year):
    dropdown_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'button[data-id="anoCalendarioSelect"]')))
    driver.execute_script("arguments[0].click();", dropdown_button)
    time.sleep(1.5)
    print("Clicking on year:", year)
    year_option = wait.until(EC.element_to_be_clickable(
        (By.XPATH, f"//span[@class='text' and normalize-space(text())='{year}']")
    ))
    time.sleep(2)
    driver.execute_script("arguments[0].click();", year_option)
    print(f"Selected (Bootstrap) year: {year}")
def select_year_native(driver, year):
    dropdown = Select(driver.find_element(By.ID, "anoCalendarioSelect"))
    dropdown.select_by_visible_text(year)
    print(f"Selected (native) year: {year}")

# --- DataFrame and Data Handling ---  
def handle_missing_table(cnpj, year, enabled_years, index, data, master_df):

    """
    Handles the case when no table is found for a given CNPJ and year.
    Marks all remaining years as not found and appends to master_df.
    Returns the updated master_df.
    """
    print(f"No table found for {cnpj} in year {year}. Skipping the rest.")
    for remaining_year in enabled_years[index:]:
        data.append({
            'cnpj': cnpj,
            'Período de Apuração': remaining_year,
            'data_found': False
        })
    missing_df = pd.DataFrame(data)
    master_df = pd.concat([master_df, missing_df], ignore_index=True)
    return master_df

# --- Logging/Reporting ---
def timings_report(start_time, total_start_time,timings):
    
    end_time = time.time() 
    elapsed = end_time - start_time
    timings.append(elapsed)
    total_elapsed = time.time() - total_start_time
    average_elapsed = sum(timings) / len(timings)
