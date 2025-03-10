import logging
import os
import shutil
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

# Configuração de logging
LOG_FILE = "cafir.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

load_dotenv()

DABASE_NAME = 'AGRONEGOCIO'
COLLECTION_NAME = 'CAFIR'
URL = 'https://arquivos.receitafederal.gov.br/dados/cafir/'
FOLDER_PATH = os.path.join(os.getcwd(), "temp/")
COLUMNS = ["NR_IMOVEL", "AREA_TOTAL", "NR_INCRA", "NOME_IMOVEL", "SIT_IMOVEL", "ENDERECO", "DISTRITO", "UF", "MUNICIPIO", "CEP", "DT_INSCRICAO", "SNCR", "CD_IMUNE"]
HOST = os.getenv("HOST")

def get_links():
    try:
        page = requests.get(URL)
        page.raise_for_status()
    except Exception as e:
        logging.error(f"Erro ao acessar a URL {URL}: {e}")
        exit(1)

    soup = BeautifulSoup(page.text, 'html.parser')
    invalid_conditions = ['?', 'pdf', '/']
    links = []
    for a in soup.find_all('a'):
        href = a.get('href', '')
        if all(cond not in href for cond in invalid_conditions):  
            full_a = urljoin(URL, href)
            links.append(full_a)

    logging.info(f"Encontrados {len(links)} links para download.")
    return links

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
        limit = -1
        for link in links:
            limit += 1
            if limit == 2:
                break
            futures.append(executor.submit(download_link, link))
        
        for future in as_completed(futures):
            future.result()

    logging.info('Download de arquivos concluido com sucesso.')

def processing(file_name):
    file_path = os.path.join(FOLDER_PATH, file_name)
    logging.info(f"Processando arquivo: {file_name}")

    df = pd.read_fwf(file_path, encoding="iso-8859-1", widths=[8,9,13,55,2,56,40,2,40,8,8,3,1], header=None, names=COLUMNS)
    df['DT_INSCRICAO'] = df['DT_INSCRICAO'].apply(lambda x : transform_date(x))
    df = replace_null_value(df)

    logging.info(f"Arquivo {file_name} processado com sucesso.")
    return df

def main():
    logging.info("Iniciando extração do CAFIR.")

    if os.path.exists(FOLDER_PATH):
        shutil.rmtree(FOLDER_PATH)
    
    os.makedirs(FOLDER_PATH)
    logging.info(f"Pasta temporária criada: {FOLDER_PATH}")

    links = get_links()
    downloads(links)

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
            df = processing(file_name)
            collection.insert_many(df.to_dict('records'))
            logging.info(f"Dados do arquivo {file_name} inseridos no MongoDB.")

        # Renomear coleção no MongoDB
        if COLLECTION_NAME in db.list_collection_names():
            db.drop_collection(COLLECTION_NAME)
            logging.info(f"Removida coleção antiga: {COLLECTION_NAME}")

        db[COLLECTION_NAME + '_ld'].rename(COLLECTION_NAME)
        logging.info(f"Renomeada coleção para: {COLLECTION_NAME}")

    except Exception as e:
        logging.error(f"Erro durante a inserção no MongoDB: {e}")
    finally:
        client.close()

    if os.path.exists(FOLDER_PATH):
        shutil.rmtree(FOLDER_PATH)
        logging.info(f"Pasta temporária removida: {FOLDER_PATH}")

    logging.info("Extração do CAFIR finalizada com sucesso.")

if __name__ == '__main__':
    main()