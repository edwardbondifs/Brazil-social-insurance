import sys
import os


import pyautogui
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
import shutil
import os

from utils import *

def main():
    # Change working directory to the project directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print("Current working directory:", os.getcwd())

    cnpj_merged = pd.read_csv('../data/MEI_numbers.csv', sep=',', encoding='utf-8')
    cnpj_merged = cnpj_merged.sample(n=5, random_state=10)
    cnpj_list = cnpj_merged['cnpj'].tolist()
    cnpj_list = [str(i) for i in cnpj_list]
    cnpj_list = ["35184782000140"]

    # Set up Chrome options
    chrome_profile_path = "C:/Temp/ChromeDebug"
    if os.path.exists(chrome_profile_path):
        shutil.rmtree(chrome_profile_path)

    url = "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/Identificacao"
    url_inside = "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/emissao"
    timings = []
    total_start_time = time.time()
    master_df = pd.DataFrame()
    master_debt_df = pd.DataFrame()  # DataFrame to store debt collection data

    for cnpj in cnpj_list:
        try:
            start_time = time.time()
            data = []

            autogui_open_page(chrome_profile_path, url,cnpj)
            driver, wait = selenium_open_page(url_inside)

            cnpj_check(driver, cnpj)

            try:
                enabled_years, use_bootstrap = get_enabled_years_bootstrap(wait, cnpj)
            except Exception:
                print("Bootstrap dropdown failed, falling back to native <select> method.")
                enabled_years, use_bootstrap = get_enabled_years_native(wait, cnpj, driver)

            print("scraping years", enabled_years)
            enabled_years.insert(0, "2010")

            obtained_pdf = 0
            for index, year in enumerate(enabled_years):
                try:
                    if use_bootstrap:
                        select_year_bootstrap(wait, driver, year)
                    else:
                        select_year_native(driver, year)

                    ok_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
                    ok_button.click()
                    time.sleep(2)

                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    table = soup.find('table', class_='table table-hover table-condensed emissao is-detailed')
                    if not table:
                        master_df = handle_missing_table(cnpj, year, enabled_years, index, data, master_df)
                        break  # Exit the year loop

                    #check if the table is subject to debt collection
                    is_debt_collector = debt_collector(soup)
                    
                    # Scrape the main table
                    new_data = scrape_data(cnpj, year, soup, table)
                    master_df = pd.concat([master_df, new_data], ignore_index=True)
                    
                    if is_debt_collector:
                    # Also scrape the debt collection table
                        print(f"Debt collection table found in year {year}.")
                        debt_data = scrape_debt_table(cnpj,soup)
                        master_debt_df = pd.concat([master_debt_df, debt_data], ignore_index=True)

                    # Check if outstanding payments exist
                    outstanding_payments = outstanding_payment(new_data)

                    # Try to obtain PDF if not in debt collection
                    if not is_debt_collector and outstanding_payments and not obtained_pdf:
                        print(f"Outstanding payments found in year {year}, period {outstanding_payments}, attempting to obtain PDF.")
                        obtain_pdf(driver,wait, outstanding_payments)
                        driver.back()
                        obtained_pdf = 1
                                
                            
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
            timings_report(start_time, total_start_time,timings)

    # Export dataframes to CSV after all CNPJs are processed
    # replace quotas with 0 if it is NaN
    master_df['Quotas'] = master_df['Quotas'].fillna(0).astype(int)
    master_df["year"] = master_df["Período de Apuração"].str.extract(r'(\d{4})').astype(int)
    #Extract month as text before '/'
    master_df["month"] = master_df["Período de Apuração"].str.extract(r'(\w+)')

    # Convert month to numerical value
    month_mapping = {
        'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4,
        'maio': 5, 'junho': 6, 'julho': 7, 'agosto': 8,
        'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12
    }
    master_df['month'] = master_df['month'].str.lower().map(month_mapping)

    master_df = master_df[['cnpj', 'Período de Apuração','year','month', 'Apurado', 'Situação', 'Benefício INSS',
            'Quotas', 'Principal', 'Multa', 'Juros', 'Total',
            'Data de Vencimento', 'Data de Acolhimento', 'data_found']]

    master_df = master_df.sort_values(by=['cnpj', 'year', 'month'])

    master_df.to_csv('../data/master_df.csv', index=False, encoding='utf-8')
    master_debt_df.to_csv('../data/master_debt_df.csv', index=False, encoding='utf-8')
    print("Data exported to ../data/master_df.csv and ../data/master_debt_df.csv")

if __name__ == "__main__":
    main()