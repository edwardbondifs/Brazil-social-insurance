
import sys
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
from multiprocessing import Pool, cpu_count

from utils import *

# def main():
#     # Change working directory to the project directory
#     os.chdir(os.path.dirname(os.path.abspath(__file__)))
#     print("Current working directory:", os.getcwd())

#     print("Starting MEI Scraper...")
#     cnpj_merged = pd.read_csv('../data/MEI_numbers.csv', sep=',', encoding='utf-8')
#     cnpj_list = cnpj_merged['cnpj'].astype(str).head(3).tolist()
#     print(f"Total CNPJ numbers to process: {len(cnpj_list)}")
#     print("CNPJ List:", cnpj_list)  

#     # # Queue batches for Celery workers
#     # print("Queuing CNPJ batches for processing...")
#     # queue_cnpj_batches(cnpj_list, batch_size=10)

#     # Directly process batches without Celery
#     batch_size = 10
#     for batch in batch_cnpjs(cnpj_list, batch_size):
#         process_cnpj_batch(batch)

# if __name__ == "__main__":
#     with Pool(2) as p:
#         # Use the pool to run the main function in parallel
#         p.map(main())

from multiprocessing import Pool, Lock, Manager
import os
import pandas as pd
from utils import *

def worker(args):
    batch, profile_id, lock = args
    chrome_profile_path = f"C:/Temp/ChromeProfile_{profile_id}"  # or "C:/Temp/ChromeProfile_{profile_id}" on Windows
    print(f"Worker {profile_id} using profile: {chrome_profile_path}")
    
    for cnpj in batch:
        # Only one worker can be on the main URL page at a time
        with lock:
            print(f"Worker {profile_id} entering critical section for CNPJ {cnpj}")
            # Here, call your function that uses PyAutoGUI to interact with the main URL page
            autogui_open_page(chrome_profile_path, "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/Identificacao", cnpj)
            print(f"Worker {profile_id} leaving critical section for CNPJ {cnpj}")
        process_cnpj_batch(chrome_profile_path, cnpj)

def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print("Current working directory:", os.getcwd())

    print("Starting MEI Scraper...")
    cnpj_merged = pd.read_csv('../data/MEI_numbers.csv', sep=',', encoding='utf-8')
    cnpj_list = cnpj_merged['cnpj'].astype(str).head(20).tolist()  # Use more for a real test
    print(f"Total CNPJ numbers to process: {len(cnpj_list)}")
    print("CNPJ List:", cnpj_list)

    batch_size = 10
    batches = list(batch_cnpjs(cnpj_list, batch_size))
    manager = Manager()
    lock = manager.Lock()
    print(f"Total batches created: {len(batches)}")
    args = [(batch, i, lock) for i, batch in enumerate(batches)]

    with Pool(3) as p:
        p.map(worker, args)

if __name__ == "__main__":
    main()