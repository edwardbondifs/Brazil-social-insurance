
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
from multiprocessing import Pool, Lock, Manager, cpu_count
import os
from Tee import Tee
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

# to do:
# Check all cnpj's work
# assign worker to dataset - is is particular workers?
# check all ports work
# Understand why bootstrap is not working


log_dir = "../data/log"
log_file = open(os.path.join(log_dir, 'output.txt'), 'w', encoding='utf-8')
sys.stdout = Tee(sys.__stdout__, log_file)
number_cnpjs = 10

def worker(args):
    try:
        batch, profile_id, lock = args
        chrome_profile_path = f"C:/Temp/ChromeProfile_{profile_id}"  # or "C:/Temp/ChromeProfile_{profile_id}" on Windows
        port = "922" + str(profile_id)  # Unique port for each worker
        port = int(port) # make port integer
        worker_id_port = str(profile_id) + "-" + str(port)
        #check port is available#
        if not is_port_available(port):
            print(f"Port {port} is not available for worker {profile_id}. Exiting.")
            return pd.DataFrame(), pd.DataFrame()
        print(f"Worker {profile_id} using profile: {chrome_profile_path}")
        print(f"worker {profile_id} processing the following cnpjs in batch: {batch}")
        all_data = pd.DataFrame()
        all_debt_data = pd.DataFrame()

        for cnpj in batch:
            try:
            # Only one worker can be on the main URL page at a time
                with lock:
                        print(f"Worker {profile_id} entering critical section for CNPJ {cnpj}")
                        time.sleep(2)
                        # Here, call your function that uses PyAutoGUI to interact with the main URL page
                        chrome_proc = autogui_open_page(chrome_profile_path, "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/Identificacao", cnpj, port)
                        print(f"Worker {profile_id} leaving critical section for CNPJ {cnpj}")
                    
                # Add a delay after leaving the lock
                delay = random.uniform(1, 2)  # 3 to 8 seconds, adjust as needed
                print(f"Worker {profile_id} sleeping for {delay:.1f} seconds before processing {cnpj}")
                time.sleep(delay)
                new_data, debt_data = process_cnpj_batch(chrome_profile_path, cnpj, port) # combines all years for a given cnpj together            
                new_data["worker_id_port"] = "id:" + str(worker_id_port)  # Add worker ID to the data
                debt_data["worker_id_port"] = "id:" + str(worker_id_port)  # Add worker ID to the debt data
                if chrome_proc:
                    chrome_proc.terminate() #closes autogui chrome
                    chrome_proc.wait()
                if new_data is not None and not new_data.empty:
                    all_data = pd.concat([all_data, new_data], ignore_index=True) # combines all data from cnpj's in a given batch together
                if debt_data is not None and not debt_data.empty:
                    all_debt_data = pd.concat([all_debt_data, debt_data], ignore_index=True)
            except:
                print(f"Error processing CNPJ {cnpj} in worker {profile_id}: {e}")
    except Exception as e:
        print(f"Worker {profile_id} encountered an error: {e}")
    return all_data, all_debt_data            

def main():
    start_time = time.time()
    master_df = pd.DataFrame() 
    master_debt_df = pd.DataFrame() 

    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print("Current working directory:", os.getcwd())

    print("Starting MEI Scraper...")
    #cnpj_merged = pd.read_csv('../data/MEI_numbers.csv', sep=',', encoding='utf-8', nrows=100)
    cnpj_merged = pd.read_csv('../data/in/MEI_numbers.csv', sep=',', encoding='utf-8', nrows=number_cnpjs)
    
    # keep only rows where used_bootstrap is false
    #cnpj_merged = cnpj_merged[cnpj_merged['used_bootstrap'] == False].drop_duplicates(subset='cnpj', keep='first')
    print(f"Total CNPJ numbers to process: {len(cnpj_merged)}")

    # Create list of CNPJ numbers to process
    cnpj_list = cnpj_merged['cnpj'].astype(str).iloc[0:number_cnpjs].to_list()  # Use more for a real test
    #cnpj_list = cnpj_merged['cnpj'].astype(str).to_list()  # Use more for a real test
    cnpj_list = ["12408064000105"]

    print(f"Total CNPJ numbers to process: {len(cnpj_list)}")
    print("CNPJ List:", cnpj_list)

    batch_size = 2
    batches = list(batch_cnpjs(cnpj_list, batch_size))
    manager = Manager()
    lock = manager.Lock()
    print(f"Total batches created: {len(batches)}")
    args = [(batch, i, lock) for i, batch in enumerate(batches)]

    #combines all batches together
    with Pool(5) as p:
        results = p.map(worker, args)
        print(f"results:{results}")
    for df, debt_df in results:
        if not df.empty:
            master_df = pd.concat([master_df, df], ignore_index=True)
        if not debt_df.empty:
            master_debt_df = pd.concat([master_debt_df, debt_df], ignore_index=True)
    store_data(master_df, master_debt_df)
    end_time = time.time()
    print(f"Total time taken: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()