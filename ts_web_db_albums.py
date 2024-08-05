import pyodbc
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import time
import os
import re

# Caminho para o ChromeDriver
chromedriver_path = "C:/WebDrivers/chromedriver.exe"

# Configuração da string de conexão com o banco de dados SQL Server
conn_str = (
    r'DRIVER={SQL Server};'
    r'SERVER=Claudio;'
    r'DATABASE=taylor_swift;'
    r'Trusted_Connection=yes;'
)

# Função para escrever log
def write_log(message):
    log_file_path = '(Seu diretório)'
    with open(log_file_path, 'a', encoding='utf-8') as log_file:
        log_file.write(message + '\n')
    print(message)  # Imprime a mensagem no console

# Função para extrair a data de atualização
def extract_date(text):
    match = re.search(r'\d{4}/\d{2}/\d{2}', text)
    if match:
        return match.group(0)
    return None

# Função para substituir "Taylor's" por "Taylor’s"
def replace_taylors(text):
    return text.replace("Taylor's", "Taylor’s")

# Função para obter os dados da página web
def get_music_data(driver):
    driver.get('https://kworb.net/spotify/artist/06HL4z0CvFAxyc27GXpf02_albums.html')
    time.sleep(5)
    
    # Capturar a data de atualização
    data_atualizacao_element = driver.find_element("xpath", '/html/body/div/div[5]')
    data_atualizacao_text = data_atualizacao_element.text
    data_atualizacao = extract_date(data_atualizacao_text)
    
    tabela = driver.find_element("xpath", '/html/body/div[1]/div[5]/table/tbody')
    dados = []

    linhas = tabela.find_elements("xpath", './/tr')
    for linha in linhas:
        elemento_clicavel = linha.find_element("xpath", './/td[1]')
        div = elemento_clicavel.find_element("xpath", './/div')
        a = div.find_element("xpath", './/a')
        link = a.get_attribute('href')

        colunas = linha.find_elements("xpath", './/td')
        nome_atual = colunas[0].text.strip()
        if nome_atual.startswith('^'):
            nome_atual = nome_atual[1:].strip()
        nome_atual = replace_taylors(nome_atual)
        total_atual = colunas[1].text.strip().replace(',', '')
        diario_atual = colunas[2].text.strip().replace(',', '')

        # Validar e substituir nome do álbum baseado na URL
        url_nome_album = {
            "https://open.spotify.com/album/6Ar2o9KCqcyYF9J0aQP3au": "Speak Now (POP Mix Version)",
            "https://open.spotify.com/album/3Mvk2LKxfhc2KVSnDYC40I": "Fearless (US Version)",
            "https://open.spotify.com/album/6tgMb6LEwb3yj7BdYy462y": "Fearless (US Version - Love Story Remix)",
            "https://open.spotify.com/album/2dqn5yOQWdyGwOpOIi9O4x": "Fearless",
            "https://open.spotify.com/album/34OkZVpuzBa9y40DCy0LPR": "1989 (Deluxe Edition - With Voice Memos)"
        }
        
        if link in url_nome_album:
            nome_atual = url_nome_album[link]

        dados.append([nome_atual, total_atual, diario_atual, link])

    return dados, data_atualizacao

# Função para verificar se a data de atualização já existe no banco de dados
def check_data_existence(cursor, data_atualizacao):
    check_query = "SELECT COUNT(*) FROM streams_albums WHERE atualizado_em = ?"
    cursor.execute(check_query, (data_atualizacao,))
    result = cursor.fetchone()
    return result[0] > 0

# Função para buscar dados do álbum no banco de dados
def get_album_data(cursor, nome_album):
    select_query = """
    SELECT nome_album, era, ano_lancamento, mes_lancamento, cor_album 
    FROM streams_albums 
    WHERE nome_album = ?
    """
    cursor.execute(select_query, (nome_album,))
    return cursor.fetchone()

# Passo 1: Configurar o ChromeDriver
service = Service(chromedriver_path)
options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
options.add_argument('--ignore-certificate-errors')
driver = webdriver.Chrome(service=service, options=options)

# Passo 2: Obter os dados da página web e a data de atualização
dados, data_atualizacao = get_music_data(driver)

# Fechar o navegador
driver.quit()

# Passo 3: Conectar ao banco de dados
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    write_log('Conexão ao banco de dados estabelecida com sucesso!')
except pyodbc.Error as e:
    error_message = f'Erro ao tentar conectar ao banco de dados: {e}'
    write_log(error_message)
    raise SystemExit(error_message)

# Passo 4: Verificar se a data de atualização já existe no banco de dados
if check_data_existence(cursor, data_atualizacao):
    write_log(f'Data de atualização {data_atualizacao} já existe no banco de dados. Encerrando a execução.')
    cursor.close()
    conn.close()
    raise SystemExit(f'Data de atualização {data_atualizacao} já existe no banco de dados.')

# Passo 5: Inserir dados no banco de dados
for nome_atual, total_atual, diario_atual, link in dados:
    album_data = get_album_data(cursor, nome_atual)
    if album_data:
        nome_album, era, ano_lancamento, mes_lancamento, cor_album = album_data
        insert_query = """
        INSERT INTO streams_albums (nome_album, era, ano_lancamento, mes_lancamento, cor_album, total_streams, streams_diarios, atualizado_em)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (nome_album, era, ano_lancamento, mes_lancamento, cor_album, total_atual, diario_atual, data_atualizacao))
        conn.commit()
        write_log(f'Registro de {nome_album} para a data {data_atualizacao} inserido no banco de dados com sucesso')
    else:
        write_log(f'Nenhum dado encontrado para o álbum: {nome_atual}')

# Passo 6: Fechar a conexão
cursor.close()
conn.close()

# Escrever "Fim" no final do log com espaçamento
write_log('Fim\n\n----------------------------------------\n\n')
