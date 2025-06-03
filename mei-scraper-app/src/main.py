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
import time

from utils import *

def main():
    # Change working directory to the project directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print("Current working directory:", os.getcwd())

    cnpj_list = load_cnpjs('../data/MEI_numbers_copy.csv')
    batch_size = 5 # Number of CNPJs to process in each batch

    # cnpj_merged = pd.read_csv('../data/MEI_numbers.csv', sep=',', encoding='utf-8')
    # cnpj_merged = cnpj_merged.sample(n=5, random_state=10)
    # cnpj_list = cnpj_merged['cnpj'].tolist()
    # cnpj_list = [str(i) for i in cnpj_list]
    # cnpj_list = ["35184782000140"]

    
    chrome_profile_path = "C:/Temp/ChromeDebug"
    url = "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/Identificacao"
    url_inside = "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/emissao"
    timings = []
    total_start_time = time.time()
    master_df = pd.DataFrame()
    master_debt_df = pd.DataFrame()  # DataFrame to store debt collection data

    for cnpj_batch in batch_cnpjs(cnpj_list, batch_size):
        master_df, master_debt_df = process_cnpj_batch(
            cnpj_batch,
            chrome_profile_path,
            url,
            url_inside,
            timings,
            total_start_time
        )

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