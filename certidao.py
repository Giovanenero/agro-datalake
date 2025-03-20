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
import cv2
import pytesseract
from datetime import datetime, timezone

LOG_FILE = "certidao.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

ENV = os.getenv("ENV", None)
DOWNLOAD_PATH = os.getenv("DOWNLOADS_PATH") if ENV != 'container' else '/app/downloads'
IMAGENS_PATH = os.getenv("IMAGENS_PATH") if ENV != 'container' else '/app/imagens'
HOST = os.getenv("HOST")
DATABASE_NAME = 'AGRONEGOCIO'
COLLECTION_NAME = 'CAFIR'
COLLECTION_ERROR = 'CIB_INVALIDO'
URL = 'https://solucoes.receita.fazenda.gov.br/Servicos/certidaointernet/ITR/Emitir'

MAP_ERROR = {
    '(Cancelado por Decisão Administrativa)': 'A emissão de certidão não foi permitida, pois o imóvel especificado foi cancelado por decisão administrativa.',
    'Certidão de Dé 4à Não foi possivel realizar a consulta': 'Não foi possível realizar a consulta. Tente mais tarde.',
    'são insuficientes para a': 'As informações disponíveis na Secretaria da Receita Federal do Brasil - RFB sobre o imóvel rural identificado pelo CIB informado são insuficientes para a emissão de certidão por meio da Internet.',
    'tente novamente dentro de alguns minutos': 'Não foi possível concluir a ação para o contribuinte informado. Por favor, tente novamente dentro de alguns minutos.',
    'nova certidão': 'Emissão de nova certidão',
    'generico': 'Não foi possível emitir a certidão para o CIB especificado.',
    'generico reemitir': 'Não foi possível reemitir a certidão para o CIB especificado.'
}

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

def process_file(file_path):    
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
        logging.exception(f"Erro ao processar {file_path} | ERROR: {e}")
        exit(1)
    finally: remove_file(file_path)

def insert_fields(collection, update_fields, cib):
    try:
        collection.update_one(
            {"NR_IMOVEL": cib},
            {"$set": update_fields}
        )
    except Exception as e:
        logging.exception(f"Erro ao atualizar documento em {COLLECTION_NAME} | CIB: {cib}")
        exit(1)

def insert_error(collection, cib, error):
    try:
        doc = {
            'NR_IMOVEL': cib,
            'ERROR': error,
            'DATA': datetime.now(timezone.utc)
        }
        collection.insert_one(doc)
    except Exception as e:
        logging.exception(f'Erro ao inserir documento em {COLLECTION_ERROR} | CIB: {cib}')
        exit(1)

def remove_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

def error_analysis(cib):
    file_name = f'{cib}.png'
    file_path = os.path.join(IMAGENS_PATH, file_name)
    try:
        while True:
            run_command('xdotool key Print', 1)
            run_command(f'xdotool type "{cib}"')
            run_command("xdotool key Return")
            #if move_file(file_name): break
            if os.path.exists(file_path): break
            run_command('xdotool key Escape')
            time.sleep(1)

        img_cv = cv2.imread(file_path, cv2.IMREAD_GRAYSCALE)
        height, width = img_cv.shape
        img_cv = img_cv[200:height-100,200:(width - 200)]
        lines = pytesseract.image_to_string(img_cv, lang="por")
        lines = lines.split('\n')
        lines = [line for line in lines if line.strip()]
        result = next((value for line in lines for key, value in MAP_ERROR.items() if key in line), False)
        if not result: return MAP_ERROR['generico']
        return False if 'nova certidão' in result else result
    except Exception as e:
        logging.exception(f'Não foi possível extrair texto da imagem | CIB: {cib}')
        exit(1)
    finally: remove_file(file_path)

def get_cibs(c, c_error):
    docs = c.aggregate([
        {"$match": {"CPF_CNPJ": None, "NR_IMOVEL": {"$ne": None}}},
    ])
    docs_error = c_error.find()
    cibs_error_set = {doc['NR_IMOVEL'] for doc in docs_error}
    cibs = [doc['NR_IMOVEL'] for doc in docs if doc['NR_IMOVEL'] not in cibs_error_set]
    return cibs

def main():
    try:
        client = MongoClient(HOST)
        collection = client[DATABASE_NAME][COLLECTION_NAME]
        collection_error = client[DATABASE_NAME][COLLECTION_ERROR]
    except Exception as e:
        logging.exception(f"Encerrando o programa, pois não foi possível criar conexão com o mongodb | ERROR: {e}")
        exit(1)

    cibs = get_cibs(collection, collection_error)
    #cibs = ['88790606', '88846946'] #cibs invalido
    try:
        for cib in cibs:
            while True:
                open_window() # abre a aba
                time.sleep(random.uniform(5, 10))

                # Preenche o campo e envia
                run_command("xdotool mousemove 350 500 click 1")
                run_command("xdotool mousemove 273 628 click 1")
                run_command(f'xdotool type "{cib}"')
                run_command("xdotool key Return")

                file_name = f"Certidao-{cib}.pdf"

                if wait_download(file_name, 5):
                    #move_file(file_name)
                    file_path = os.path.join(DOWNLOAD_PATH, file_name)
                    update_fields = process_file(file_path)
                    insert_fields(collection, update_fields, cib)
                    logging.info(f'CIB: {cib} | Emitido com sucesso.')
                    remove_file(file_path)
                    break

                # Se não baixou, analisa erro
                result = error_analysis(cib)
                if result:
                    if 'tente novamente dentro de alguns minutos' in result.lower():
                        logging.info(f'Bloqueado, esperando 30min...')
                        time.sleep(1800)  # Bloqueado, espera 30 min
                        logging.info(f'Retomando a extração.')
                        continue
                    else:
                        insert_error(collection_error, cib, result)
                    logging.error(f'CIB: {cib} | Erro detectado: {result}')
                    break

                # Tenta reemitir
                run_command("xdotool mousemove 589 472 click 1")
                run_command("xdotool key Tab", 0.1)
                run_command("xdotool key Tab", 0.1)
                run_command("xdotool key Return")

                if not wait_download(file_name, 5):
                    insert_error(collection_error, cib, MAP_ERROR['generico reemitir'])
                    logging.error(f'CIB: {cib} | Não foi emitido.')
                else:
                    logging.info(f'CIB: {cib} | Emitido com sucesso.')
                    remove_file(file_path)
                break

            run_command("xdotool key Ctrl+w")  # Fecha aba
    except Exception as e:
        logging.exception(f"Encerrando o programa, pois não foi possível emitir as certidões pelo CIB | ERROR: {e}")
    finally:
        if client: client.close()
if __name__ == '__main__':
    main()