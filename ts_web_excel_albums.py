from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import time
import pandas as pd
import os

# Caminho para o ChromeDriver
chromedriver_path = "C:/WebDrivers/chromedriver.exe"

# Obtém o diretório do arquivo Python principal
script_dir = os.path.dirname(os.path.abspath(__file__))

# Caminho do arquivo Excel
caminho_excel = os.path.join(script_dir, "TS_table.xlsx")

service = Service(chromedriver_path)
options = webdriver.ChromeOptions()

driver = webdriver.Chrome(service=service, options=options)

# Configurações do navegador
options = Options()
options.add_argument('--ignore-certificate-errors')
# driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")  # Maximize a janela do navegador ao iniciar
# service = Service(executable_path='./chromedriver.exe')
driver = webdriver.Chrome(service=service, options=options)

# Abrir o site
driver.get('https://kworb.net/spotify/artist/06HL4z0CvFAxyc27GXpf02_albums.html')

# Esperar um pouco para garantir que a página seja totalmente carregada
time.sleep(5)

# Encontrar a tabela
tabela = driver.find_element("xpath", '/html/body/div[1]/div[5]/table/tbody')

# Lista para armazenar os dados da tabela
dados = []

# Percorrer cada linha da tabela
linhas = tabela.find_elements(By.TAG_NAME, "tr")
for linha in linhas:
    # Encontrar o elemento que contém o texto clicável
    elemento_clicavel = linha.find_element("xpath", './/td[1]')   

    # Encontrar o elemento da div dentro da linha
    div = elemento_clicavel.find_element("xpath", './/div')

    # Encontrar o elemento da a dentro da linha
    a = div.find_element("xpath", './/a')

    # Extrair o link desse elemento
    link = a.get_attribute('href')

    # Extrair os dados da coluna 1, 2 e 3
    colunas = linha.find_elements(By.TAG_NAME, "td")
    nome_atual = colunas[0].text.strip()
    if nome_atual.startswith('^'):
        nome_atual = nome_atual[1:].strip()
    total_atual = colunas[1].text.strip()
    diario_atual = colunas[2].text.strip()
    print(nome_atual + ", " + total_atual + ", " + diario_atual + ", " + str(link))

    # Verificar se o link capturado é igual a um URL específico
    if link == "https://open.spotify.com/intl-pt/album/6tgMb6LEwb3yj7BdYy462y":
        # Fazer algo se o link for igual a este URL
        print("O link capturado é igual a https://open.spotify.com/intl-pt/album/6tgMb6LEwb3yj7BdYy462y")

    # Verificar se o link capturado é igual a outro URL específico
    elif link == "outro_link":
        # Fazer algo se o link for igual a outro URL
        print("O link capturado é igual a outro_link")

    # Adicione mais condições 'elif' conforme necessário para comparar com outros URLs específicos

    # Adicionar dados à lista
    dados.append([nome_atual, total_atual, diario_atual, link])

# Criar um DataFrame do pandas com os dados coletados
df_novo = pd.DataFrame(dados, columns=['Nome', 'Total', 'Diario', 'Link'])

# Calcular a data de ontem
data_ontem = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')

# Adicionar coluna com a data de ontem
df_novo['Data'] = data_ontem

# Verificar se o arquivo Excel já existe
if os.path.exists(caminho_excel):
    # Carregar o arquivo Excel existente
    df_existente = pd.read_excel(caminho_excel)
    # Concatenar os novos dados com os dados existentes
    df_resultante = pd.concat([df_existente, df_novo], ignore_index=True)
else:
    # Se o arquivo não existir, usar apenas os novos dados
    df_resultante = df_novo

# Salvar o arquivo Excel resultante
df_resultante.to_excel(caminho_excel, index=False)

# Fechar o navegador
driver.quit()
