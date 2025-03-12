import os
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import random
 
def main():
    options = uc.ChromeOptions()
    profile = '/home/giovane/.config/google-chrome/Profile 1'
    options.add_argument(f"--user-data-dir={profile}")
    options.add_argument(f'--load-extension={os.path.abspath("hekt")}')
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
    #options.add_argument("--no-sandbox")
    #options.add_argument("--disable-gpu")
    #options.add_argument("--disable-dev-shm-usage")

    driver = uc.Chrome(options=options) # usando undetected chrome-driver, vai eliminar/esconder a maioria dos metadados que o selenium (ou qualquer tipo de automação deixa)
    driver.maximize_window()

    url = 'https://solucoes.receita.fazenda.gov.br/Servicos/certidaointernet/ITR/Emitir'
    driver.get(url)
    time.sleep(random.uniform(1, 8)) # simulando comportamento humano (tempo pra mexer na página)
   
    WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.ID, 'NI')))
    time.sleep(random.uniform(1, 5)) # simulando comportamento humano (tempo pra mexer na página)
   
    inputcib = driver.find_element(By.ID, 'NI')
    inputcib.send_keys('22749683')
    time.sleep(random.uniform(1, 5)) # simulando comportamento humano (tempo pra mexer na página)
    submitbutton = driver.find_element(By.ID, 'validar')
    time.sleep(random.uniform(1, 5))# simulando comportamento humano (tempo pra mexer na página)
    submitbutton.send_keys(Keys.ENTER)
    time.sleep(200) # simulando comportamento humano (tempo pra mexer na página)
 
    driver.quit()
 
if __name__ == "__main__":
    main()