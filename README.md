# Streaming Data Automation

Este repositório contém dois scripts Python que automatizam a extração e armazenamento de dados de streaming de álbuns do artista.

## Scripts

### 1. `ts_web_db_albums`

Este script automatiza a extração de dados de uma página web específica que lista álbuns e suas estatísticas de streaming no Spotify. Utilizando Selenium para navegar e extrair informações da página, e PyODBC para interagir com um banco de dados SQL Server, o script verifica se os dados já existem no banco antes de inseri-los. Se novos dados forem encontrados, eles são inseridos na tabela `streams_albums` no banco de dados. O script também mantém um log detalhado das operações realizadas.

**Objetivo principal**: Automatizar a coleta e inserção de dados de streaming de álbuns do artista, garantindo que novas informações sejam registradas sem duplicação no banco de dados.

### 2. `ts_web_excel_albums`

Este script também automatiza a extração de dados de uma página web específica que lista álbuns e suas estatísticas de streaming no Spotify. Utilizando Selenium para navegar e extrair informações da página, o script armazena os dados extraídos em um arquivo Excel. Se o arquivo Excel já existir, ele adiciona os novos dados aos dados existentes, garantindo que a data de coleta seja registrada para cada conjunto de dados.

**Objetivo principal**: Automatizar a coleta e o registro de dados de streaming de álbuns do artista em um arquivo Excel, facilitando o acompanhamento e a análise contínua dessas informações.

## Estrutura do Repositório

├── ts_web_db_albums.py
├── ts_web_excel_albums.py
├── TS_table.xlsx
└── README.md


## Requisitos

- Python 3.x
- Bibliotecas Python:
  - `selenium`
  - `pyodbc`
  - `pandas`
  - `webdriver_manager`
- ChromeDriver

## Configuração

1. Clone o repositório:
   ```bash
   git clone https://github.com/ClaudioLimahub/python_Albuns_TS.git

2. Instale as bibliotecas necessárias:
pip install selenium pyodbc pandas webdriver_manager

3. Configure o caminho para o ChromeDriver no script ts_web_db_albums.py e ts_web_excel_albums.py:
chromedriver_path = "C:/WebDrivers/chromedriver.exe"

4. Ajuste a string de conexão com o banco de dados no script ts_web_db_albums.py conforme necessário.

# Uso
## Executar ts_web_db_albums.py
Este script irá extrair dados de streaming e inseri-los no banco de dados SQL Server.
python ts_web_db_albums.py

## Executar ts_web_excel_albums.py
Este script irá extrair dados de streaming e armazená-los em um arquivo Excel.
python ts_web_excel_albums.py

## Contribuição
Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests.
