import logging
import os
import shutil
from time import sleep
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import os
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import undetected_chromedriver as webdriver

# Configuração de logging
LOG_FILE = "cafir-scnr.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

load_dotenv()

DABASE_NAME = 'AGRONEGOCIO'
COLLECTION_NAME = 'CAFIR_CAFIR'
URL_CAFIR = 'https://arquivos.receitafederal.gov.br/dados/cafir/'
URL_SCNR = 'https://sncr.serpro.gov.br/sncr-web/consultaPublica.jsf?windowId=00a'
FOLDER_PATH = os.getenv("DOWNLOADS_PATH") if os.getenv("ENV", None) != 'container' else '/app/downloads'
COLUMNS = ["NR_IMOVEL", "AREA_TOTAL", "NR_INCRA", "NM_IMOVEL", "SIT_IMOVEL", "ENDERECO", "NM_DISTRITO", "SG_UF", "NM_MUNICIPIO", "CEP", "DT_INSCRICAO", "IN_INSENTO", "CD_SCNR"]
HOST = os.getenv("HOST")

def get_links_cafir():
    """Coleta todos os links e data dos arquivos do CAFIR da Paraíba"""
    try:
        chrome_options = Options()
        chrome_options.add_argument('--headless') 
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(URL_CAFIR)
        sleep(1)
    except Exception as e:
        logging.error(f"Erro ao acessar a URL {URL_CAFIR}: {e}")
        exit(1)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    invalid_conditions = ['?', 'pdf', '/']
    tbody = soup.find('tbody')
    datas = {}
    for tr in tbody.find_all('tr'):
        a = tr.find('a')
        if not a:
            continue

        href = a.get('href', '')
        if all((cond not in href) and ('.PB' in href) for cond in invalid_conditions):  
            full_a = urljoin(URL_CAFIR, href)
            date = tr.find_all('td')[2].text.strip()
            date = datetime.strptime(date, "%Y-%m-%d %H:%M")
            date = date.strftime("%Y-%m-%dT%H:%M:%S.000+00:00")
            datas[href] = {
                'link': full_a,
                'date': date
            }

    logging.info(f"Encontrados {len(datas)} links para download.")
    return datas

def dir_temp(create:bool,clean:bool):
    if clean and os.path.exists(FOLDER_PATH):
        shutil.rmtree(FOLDER_PATH)
        logging.info(f"Removendo Pasta temporária: {FOLDER_PATH}")
    
    if create and not os.path.exists(FOLDER_PATH):
        os.makedirs(FOLDER_PATH)
        logging.info(f"Pasta temporária criada: {FOLDER_PATH}")

def download_link(url):
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        file_name = url.split('/')[-1]
        file_path = os.path.join(FOLDER_PATH, file_name)

        with open(file_path, 'wb') as f:
            f.write(response.content)

        logging.info(f"Download do arquivo {file_name} concluído.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro no download de {url}: {e}")

def replace_null_value(df:pd.DataFrame):
    ''' Troca valores nulos ou vazios por None '''
    df.replace(np.nan, None, inplace=True) 
    df.fillna('None', inplace=True)
    df.replace('None', None, inplace=True) 
    df.replace('nan', None, inplace=True) 
    df.replace('', None, inplace=True)
    return df

def transform_date(date:int):
    try:
        if date == 0:
            return None
            
        if str(date)[:4] != '1900':
            return datetime.strptime(str(date), '%Y%m%d')

    except Exception as e:
        logging.error(f"Erro ao transformar data {date}: {e}")
    
    return None

def downloads(links):   
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = []
        for link in links:
            futures.append(executor.submit(download_link, link))
        
        for future in as_completed(futures):
            future.result()

    logging.info('Download de arquivos concluido com sucesso.')

def dataframe_processing(file_name, date:datetime):
    file_path = os.path.join(FOLDER_PATH, file_name)
    logging.info(f"Processando arquivo: {file_name}")

    df = pd.read_fwf(file_path, encoding="iso-8859-1", widths=[8,9,13,55,2,56,40,2,40,8,8,3,1], header=None, names=COLUMNS)
    df['DT_INSCRICAO'] = df['DT_INSCRICAO'].apply(lambda x : transform_date(x))
    df = replace_null_value(df)

    df['NM_ARQUIVO'] = file_name
    df['DT_ARQUIVO'] = pd.to_datetime(date)
    df['CPF_CNPJ'] = None
    df['NM_CONTRIBUINTE'] = None
    df['IN_CPF'] = None
    df['CD_MUNICIPIO'] = None
    df['DENOMINACAO_IMOVEL'] = None
    df['NATUREZA_JURIDICA'] = None
    df['CONDICAO_PESSOA'] = None
    df['PERCENTUAL_DETENCAO'] = None

    logging.info(f"Arquivo {file_name} processado com sucesso.")
    return df

def database_processing(collection):
    """
        Remove os documentos duplciados pelo NR_IMOVEL, deixando apenas o mais
        mais recente de acordo com a DT_ARQUIVO, além de remover do campo NR_IMOVEL
        os caracteres
    """
    logging.info("Iniciando processamento do banco de dados.")
    field = 'NR_IMOVEL'
    # filtra os elementos duplicados
    pipeline = [
        {"$sort": {"DT_ARQUIVO": -1}},
        {"$group": {
            "_id": f"${field}",
            "ids": {"$push": "$_id"},
            "first_id": {"$first": "$_id"}
        }},
        {"$project": {
            "ids": {
                "$filter": {
                    "input": "$ids",
                    "as": "id",
                    "cond": {"$ne": ["$$id", "$first_id"]}
                }
            }
        }}
    ]
    logging.info("Removendo registros com o cib duplicado.")
    duplicates = list(collection.aggregate(pipeline, allowDiskUse=True))
    # Remover os documentos duplicados, mantendo apenas o mais recente
    for doc in duplicates:
        if doc["ids"]:
            collection.delete_many({"_id": {"$in": doc["ids"]}})

    logging.info("Removendo registros com o cib com caracter.")
    # atualiza os documentos com o NR_IMOVEL com caracter para None
    collection.update_many(
        {"NR_IMOVEL": {"$regex": "[^0-9]"}},
        {"$set": {field: None}}
    )

    logging.info("Finalizando o processamento do banco de dados.")

def cafir_extraction():
    logging.info("Iniciando extração do CAFIR.")

    dir_temp(True, True)

    datas = get_links_cafir()
    downloads(list(map(lambda x: x['link'], datas.values())))

    try:
        client = MongoClient(HOST)
        db = client[DABASE_NAME]
        collection = db[COLLECTION_NAME + '_ld']
    except Exception as e:
        logging.error(f"Erro ao abrir conexão com o MongoDB: {e}")
        exit(1)

    try:
        # Processamento dos dataframes e inserção no banco de dados
        for file_name in os.listdir(FOLDER_PATH):
            df = dataframe_processing(file_name, datas[file_name]['date'])
            collection.insert_many(df.to_dict('records'))
            logging.info(f"Dados do arquivo {file_name} inseridos no MongoDB.")

        database_processing(collection)

    except Exception as e:
        logging.error(f"Erro durante a inserção no MongoDB: {e}")
    finally:
        client.close()

    dir_temp(False, True)
    logging.info("Extração do CAFIR finalizada com sucesso.")

def wait_download(timeout:int=300):
    """Aguarda até que o arquivo seja completamente baixado."""
    sleep(2)
    start_time = time.time()
    while True:
        if not any(filename.endswith(".crdownload") for filename in os.listdir(FOLDER_PATH)):
            break
        if time.time() - start_time > timeout:
            logging.error("O download demorou muito para finalizar.")
            exit(1)
        sleep(1)
    return True

def update_database(df: pd.DataFrame):
    client = MongoClient(HOST)
    collection = client[DABASE_NAME][COLLECTION_NAME + '_ld']
    #collection = client[DABASE_NAME][COLLECTION_NAME]

    def parse_area(area):
        """Converte string de área para float corretamente"""
        return float(str(area).replace('.', '').replace(',', '.')) if area else None

    datas = list(collection.find())
    datas_dict = {data["NR_INCRA"]: data for data in datas}
    new_datas = []
    for i, row in df.iterrows():
        cod = row.get("CÓDIGO DO IMOVEL")
        if not cod:
            continue

        area_total = parse_area(row.get("ÁREA TOTAL", "0"))
        natureza = row.get("NATUREZA JURÍDICA", None)
        in_cpf = True if natureza else None
        condicao = row.get("CONDIÇÃO DA PESSOA", None)
        result = {
            "DENOMINACAO_IMOVEL": str(row.get("DENOMINAÇÃO DO IMÓVEL", "")).upper() or None,
            "CD_MUNICIPIO": row.get("CÓDIGO DO MUNICÍPIO (IBGE)"),
            "AREA_TOTAL": area_total,
            "NM_CONTRIBUINTE": row.get("TITULAR"),
            "NATUREZA_JURIDICA": str(natureza).upper() if natureza else None,
            "CONDICAO_PESSOA": str(condicao).upper() if condicao else None,
            "PERCENTUAL_DETENCAO": row.get("PERCENTUAL DE DETENÇÃO"),
            "IN_CPF": in_cpf
        }

        if cod in datas_dict:
            # Atualiza o imóvel existente
            datas_dict[cod].update(result)
        else:
            # Adiciona um novo imóvel
            result.update({
                "NR_IMOVEL": None,
                "NM_IMOVEL": None,
                "SIT_IMOVEL": None,
                "ENDERECO": None,
                "NM_DISTRITO": None,
                "SG_UF": row.get("UF", None),
                "NM_MUNICÍPIO": row.get("MUNICÍPIO", None),
                "CEP": None,
                "DT_INSCRICAO": None,
                "IN_INSENTO": None,
                "CD_SCNR": None,
                "NM_ARQUIVO": None,
                "DT_ARQUIVO": None,
                "CPF_CNPJ": None
            })
            new_datas.append(result)

    # Atualiza a lista com os dados novos
    datas.extend(new_datas)
    drop_collection(COLLECTION_NAME + '_ld')
    collection.insert_many(datas)

def sncr_extraction():
    logging.info("Iniciando a extração do SCNR.")

    #dir_temp(True, True)

    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": FOLDER_PATH,
        "download.prompt_for_download": False, 
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    #options.add_argument('--headless') 
    options.add_argument(f'--load-extension={os.path.abspath("./../hekt")}') # usado para resolver o hcaptcha
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    driver.get(URL_SCNR)
    sleep(20)

    select = Select(driver.find_element(By.ID, 'selectUf'))
    select.select_by_visible_text("Paraíba")

    sleep(1)

    select = Select(driver.find_element(By.ID, 'selectMunicipio'))
    municipios = [option.text for option in select.options]
    print(municipios)
    for municipio in municipios:
        sleep(5)
        select = Select(driver.find_element(By.ID, 'selectUf'))
        select.select_by_visible_text("Paraíba")

        select = Select(driver.find_element(By.ID, 'selectMunicipio'))
        select.select_by_visible_text(municipio)
        button = driver.find_element(By.ID, 'botaoDownload')
        button.click()

        if not wait_download():
            driver.quit()
            logging.error('Erro ao fazer download dos arquivos do SNCR em {}'.format(URL_SCNR))
            exit(1)

        driver.refresh()
    

    logging.info('Download dos arquivos do SNCR realizado com sucesso.')
    driver.quit()

    file_names = os.listdir(FOLDER_PATH)

    if len(file_names) == 0:
        logging.error('Não existe arquivos para processar')
        exit(1)

    file_names = [os.path.join(FOLDER_PATH, file_name) for file_name in file_names if file_name.endswith('.csv')]
    df = pd.DataFrame()
    for file_name in file_names:
        df_aux = pd.read_csv(file_name, sep=';', encoding='utf8')
        df = pd.concat([df, df_aux]).drop_duplicates().reset_index(drop=True)
        
    df = replace_null_value(df)
    logging.info("Iniciando processamento {}".format(file_name))
    update_database(df)
    logging.info("Finalizando processamento {}".format(file_name))

    dir_temp(False, True)

    logging.info("Finalizando a extração do SCNR.")

def create_index():
    try:
        client = MongoClient(HOST)
        collection = client[DABASE_NAME][COLLECTION_NAME]
        collection.create_index("NR_IMOVEL")
        collection.create_index("NR_INCRA")
        collection.create_index("CPF")
        collection.create_index("NM_CONTRIBUINTE")
    except Exception as e:
        logging.error('Não foi possível criar índices')
    finally:
        if client: client.close()

def drop_collection(name_drop:str):
    try:
        client = MongoClient(HOST)
        db = client[DABASE_NAME]
        if name_drop in db.list_collection_names():
            db.drop_collection(name_drop)
            logging.info(f"Removida coleção antiga: {name_drop}")
    except Exception as e:
        logging.error('Não foi dropar a coleção: {}'.format(e))
        exit(1)
    finally:
        if client: client.close()

def replace_collection(name_old:str):
    try:
        client = MongoClient(HOST)
        db = client[DABASE_NAME]
        if COLLECTION_NAME in db.list_collection_names():
            db.drop_collection(COLLECTION_NAME)
            logging.info(f"Removida coleção antiga: {COLLECTION_NAME}")
        db[name_old].rename(COLLECTION_NAME)
        logging.info(f"Renomeada a coleção {name_old} para {COLLECTION_NAME} com sucesso.")
    except Exception as e:
        logging.error('Não foi possível renomear a coleção: {}'.format(e))
        exit(1)
    finally:
        if client: client.close()

def main():
    collection_name = COLLECTION_NAME + '_ld'
    drop_collection(name_drop=collection_name)
    cafir_extraction()
    sncr_extraction()
    replace_collection(name_old=collection_name)
    #create_index()

if __name__ == '__main__':
    main()