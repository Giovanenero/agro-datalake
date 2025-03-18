import os
import random
import re
import shutil
import time
import subprocess
import time
import PyPDF2
from pymongo import MongoClient
import logging

LOG_FILE = "certidao.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

HOST = os.getenv("HOST")
DATABASE_NAME = 'AGRONEGOCIO'
COLLECTION_NAME = 'CAFIR'
URL = 'https://solucoes.receita.fazenda.gov.br/Servicos/certidaointernet/ITR/Emitir'
client = MongoClient(HOST)
collection = client[DATABASE_NAME][COLLECTION_NAME]
DOWNLOAD_PATH = "/home/giovane/Downloads"
FOLDER_PATH = os.path.join(os.getcwd(), 'certidoes')
LIMIT = 200

def run_command(command, sleep=0.5):
    """Executa um comando no shell"""
    subprocess.run(command, shell=True)
    time.sleep(sleep)

def open_window():
    run_command('xdotool search --onlyvisible --class "chrome" windowactivate')
    run_command("xdotool key ctrl+t")
    time.sleep(1)

    window_id = os.popen("xdotool search --onlyvisible --class 'chrome' | head -n 1").read().strip()

    run_command(f'xdotool type "{URL}"')
    run_command("xdotool key Return")
    time.sleep(2)

    run_command(f"xdotool windowactivate {window_id}")

def wait_download(file_name, timeout=30):
    start_time = time.time()
    file_path = os.path.join(DOWNLOAD_PATH, file_name)
    while time.time() - start_time < timeout:
        if os.path.exists(file_path) and not file_path.endswith(('.crdownload', '.part')):
            return True
        time.sleep(1)
    return False

def move_file(file_name):
    file_path = os.path.join(DOWNLOAD_PATH, file_name)
    if os.path.exists(file_path):
        shutil.move(file_path, FOLDER_PATH)

def process_file(file_path):
    if not os.path.exists(file_path):
        logging.error(f"Caminho para o arquivo {file_path} não existe")
        return None
    
    try:
        file = open(file_path, 'rb')
        pdf = PyPDF2.PdfReader(file)
        lines = pdf.pages[0].extract_text().split('\n')
        nm_imovel = lines[6].replace('Nome do Imóvel: ', '').upper().strip()
        municipio = re.sub(r'Município: | UF: \w{2}', '', lines[8]).upper().strip()   
        area = float(re.sub(r'[^0-9,]', '', lines[9]).replace(',', '.'))
        contribuinte = lines[11].replace('Contribuinte: ', '').strip()
        in_cpf = 'CPF' in lines[12]
        cpf_cnpj = re.sub(r'\D', '', lines[12]) 

        return {
            'AREA_TOTAL': area,
            'NM_IMOVEL': nm_imovel,
            'NM_MUNICIPIO': municipio,
            'CPF_CNPJ': cpf_cnpj,
            'NM_CONTRIBUINTE': contribuinte,
            'IN_CPF': in_cpf
        }
    except Exception as e:
        logging.error(f"Erro ao processar {file_path} | ERROR: {e}")
    return None

def insert_fields(collection, update_fields, cib):
    try:
        collection.update_one(
            {"NR_IMOVEL": cib},
            {"$set": update_fields}
        )
        logging.info(f"Documento atualizado com sucesso | CIB: {cib}")
    except Exception as e:
        logging.info(f"Erro ao atualizar docuemnto | CIB: {cib} | ERROR: {e}")

def remove_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

def update_count(count:int):
    count += 1
    if count >= LIMIT:
        count = 0
        time.sleep(1800) # espera 30min, pois assim deve evitar o bloqueio por parte do site
    return count

def main():
    try:
        client = MongoClient(HOST)
        collection = client[DATABASE_NAME][COLLECTION_NAME]
        docs = collection.aggregate([
            {"$match": {"CPF_CNPJ": None, "NR_IMOVEL": {"$ne": None}}},
            {"$sample": {"size": 1000}}
        ])
        cibs = [doc['NR_IMOVEL'] for doc in docs]
    except Exception as e:
        logging.error(f"Encerrando o programa, pois não foi possível criar conexão com o mongodb | ERROR: {e}")
        exit(1)
        
    os.makedirs(FOLDER_PATH, exist_ok=True)
    count = -1

    try:
        for cib in cibs:
            count = update_count(count)
            open_window()
            time.sleep(random.uniform(5, 15))

            run_command("xdotool mousemove 350 500 click 1")
            logging.info(f'Coletando | CIB: {cib}')
            run_command("xdotool mousemove 273 628 click 1")
            run_command(f'xdotool type "{cib}"')
            run_command("xdotool key Return")

            file_name = f"Certidao-{cib}.pdf"
            download = True
            if not wait_download(file_name, 5):
                run_command("xdotool mousemove 589 472 click 1")
                run_command("xdotool key Tab", 0.1)
                run_command("xdotool key Tab", 0.1)
                run_command("xdotool key Return")

                if not wait_download(file_name, 5):
                    logging.error(f'Não foi possível emitir a certidão.| CIB: {cib}')
                    download = False
                else:
                    logging.info(f'Emissão da certidão realizada com sucesso. | CIB: {cib}')

            run_command("xdotool key Ctrl+w")
            if not download:
                continue
            
            move_file(file_name)
            file_path = os.path.join(FOLDER_PATH, file_name)
            update_fields = process_file(file_path)
            remove_file(file_path)
            if not update_fields:
                continue
            
            insert_fields(collection, update_fields, cib)
    except Exception as e:
        logging.error(f"Encerrando o programa, pois não foi possível emitir as certidões pelo CIB | ERROR: {e}")
    finally:
        if os.path.exists(FOLDER_PATH): shutil.rmtree(FOLDER_PATH)

if __name__ == '__main__':
    main()