import os
import cv2
import pytesseract

MAP_ERROR = {
    '(Cancelado por Decisão Administrativa)': 'A emissão de certidão não foi permitida, pois o imóvel especificado foi cancelado por decisão administrativa.',
    'Certidão de Dé 4à Não foi possivel realizar a consulta': 'Não foi possível realizar a consulta. Tente mais tarde.',
    'são insuficientes para a': 'As informações disponíveis na Secretaria da Receita Federal do Brasil - RFB sobre o imóvel rural identificado pelo CIB informado são insuficientes para a emissão de certidão por meio da Internet.',
    'tente novamente dentro de alguns minutos': 'Não foi possível concluir a ação para o contribuinte informado. Por favor, tente novamente dentro de alguns minutos.',
    'nova certidão': 'Emissão de nova certidão',
    'generico': 'Não foi possível emitir a certidão para o CIB especificado.'
}

IMAGENS_PATH = os.getenv("IMAGENS_PATH")
FILE_NAME = os.path.join(IMAGENS_PATH, '')

# Carrega a imagem usando OpenCV
img_cv = cv2.imread(FILE_NAME, cv2.IMREAD_GRAYSCALE)
height, width = img_cv.shape
img_cv = img_cv[200:height-100,200:(width - 200)]

cv2.imwrite("save.png", img_cv)

lines = pytesseract.image_to_string(img_cv, lang="por")
lines = lines.split('\n')
lines = [line for line in lines if line.strip()]

for line in lines: print(line)

error = next((value for line in lines for key, value in MAP_ERROR.items() if key in line), None)
print(error)