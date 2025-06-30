def selenium_open_page_and_login(chrome_profile_path, url, cnpj):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import time

    options = Options()
    options.add_argument(f"--user-data-dir={chrome_profile_path}")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-first-run")
    options.add_argument("--no-default-browser-check")
    # options.add_argument("--headless")  # Uncomment for headless mode

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    driver.get(url)
    # Wait for the CNPJ input to be present
    wait.until(EC.presence_of_element_located((By.NAME, "cnpj")))
    cnpj_input = driver.find_element(By.NAME, "cnpj")
    cnpj_input.clear()
    cnpj_input.send_keys(cnpj)

    # Wait for the "Continuar" button to be enabled and click it
    continuar_btn = wait.until(EC.element_to_be_clickable((By.ID, "continuar")))
    continuar_btn.click()

    # Wait for navigation or next page to load as needed
    time.sleep(2)
    return driver, wait

def kill_chrome(): 
    """Kill all Chrome processes.""" 
    for proc in psutil.process_iter(['pid', 'name']): 
        if 'chrome' in proc.info['name'].lower(): 
            try: 
                proc.kill() 
            except psutil.NoSuchProcess: 
                pass 

def process_cnpj_batch_playwright(cnpj_batch):
    import pandas as pd
    import time
    from playwright.sync_api import sync_playwright
    from playwright_stealth import stealth_sync

    url = "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/Identificacao"
    url_inside = "https://www8.receita.fazenda.gov.br/SimplesNacional/Aplicacoes/ATSPO/pgmei.app/emissao"
    timings = []
    total_start_time = time.time()
    master_df = pd.DataFrame()
    master_debt_df = pd.DataFrame()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # Set to True for headless
        context = browser.new_context()
        stealth_sync(context)
        page = context.new_page()

        for cnpj in cnpj_batch: 
            start_time = time.time()
            data = []

            # Step 1: Go to the main page and enter CNPJ
            page.goto(url)
            page.wait_for_selector('input[name="cnpj"]', timeout=10000)
            page.fill('input[name="cnpj"]', cnpj)
            page.click('button[type="submit"]')
            time.sleep(2)
            print(page.content())
            page.wait_for_url(url_inside, timeout=15000)
            time.sleep(2)
            print(page.content())

            # Step 2: Now on the inside page
            # Get enabled years (try bootstrap first, then fallback)
            try:
                page.wait_for_selector('button[data-id="anoCalendarioSelect"]', timeout=5000)
                page.click('button[data-id="anoCalendarioSelect"]')
                time.sleep(1)
                year_elements = page.query_selector_all(".dropdown-menu.inner li a span.text")
                enabled_years = [el.inner_text().strip() for el in year_elements if el.inner_text().strip()]
                enabled_years = [year for year in enabled_years if "Não optante" not in year]
                use_bootstrap = True
            except Exception:
                select = page.query_selector("#anoCalendarioSelect")
                enabled_years = [option.inner_text().strip() for option in select.query_selector_all("option") if option.inner_text().strip()]
                enabled_years = [year for year in enabled_years if "Não optante" not in year]
                use_bootstrap = False
                

def select_year_bootstrap(cnpj, wait, driver, year, retries=3, delay=2):
    year = str(year)
    for attempt in range(1, retries+1):
        try:
            print(f"{cnpj} Attempt {attempt} to pick {year}")

            wait = WebDriverWait(driver, 10)

            # 1) Open the dropdown
            print("opening dropdown")
            btn = wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, 'button[data-id="anoCalendarioSelect"]')))
            btn.click()

            print("click list item a")
            # 2) Click the list-item’s <a>
            opt = wait.until(EC.element_to_be_clickable((
                By.XPATH,
                f"//li/a[normalize-space(.)='{year}']"
            )))
            opt.click()

            # 3) Submit
            print("submitting")
            ok = wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "button[type='submit']")))
            ok.click()

            print(f"{cnpj} - Successfully selected {year}")
            time.sleep(1.5)
            return

        except (TimeoutException, ElementClickInterceptedException) as e:
            print(f"{cnpj} - Failed attempt {attempt}: {e}")
            time.sleep(delay)
    try:
        # If all attempts fail, try to select the year using JavaScript
        print(f"{cnpj} - All attempts failed, trying JS setValue for {year}")
        driver.execute_script("""
            var sel = document.getElementById('anoCalendarioSelect');
            sel.value = arguments[0];
            $(sel).selectpicker('refresh').trigger('changed.bs.select');
        """, year)
        driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
        time.sleep(1.5)
    except Exception as js_e:
        print(f"{cnpj} - JS setValue failed for {year}: {js_e}")
        raise Exception(f"Failed to select year {year} after {retries} attempts and JS fallback.")
def select_year_native(wait, driver, year, retries=3, delay=2):