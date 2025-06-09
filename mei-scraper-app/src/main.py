
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

from utils import *

def main():
    # Change working directory to the project directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    print("Current working directory:", os.getcwd())

    print("Starting MEI Scraper...")
    cnpj_merged = pd.read_csv('../data/MEI_numbers.csv', sep=',', encoding='utf-8')
    cnpj_list = cnpj_merged['cnpj'].astype(str).head(5).tolist()
    print(f"Total CNPJ numbers to process: {len(cnpj_list)}")
    print("CNPJ List:", cnpj_list)  

    # # Queue batches for Celery workers
    # print("Queuing CNPJ batches for processing...")
    # queue_cnpj_batches(cnpj_list, batch_size=10)

    # Directly process batches without Celery
    batch_size = 10
    for batch in batch_cnpjs(cnpj_list, batch_size):
        process_cnpj_batch(batch)

if __name__ == "__main__":
    main()

