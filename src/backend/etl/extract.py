import requests
import pandas as pd
from bs4 import BeautifulSoup
from os import path
import logging

from src.config import Config

# Set up logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Extract:
    """Class to execute data extraction of complaints from the platform Consumidor.gov.br"""
    
    def __init__(self, lines=50_000, quantity=10, data_inicio="", data_termino=""):
        """
        Args:
            lines: número de reclamações a extrair (paginação de 10 em 10).
            quantity: resultados por página (10 na API).
            data_inicio: filtro de data inicial (ex: "01/06/2025"). Vazio = sem filtro.
            data_termino: filtro de data final (ex: "31/07/2025"). Vazio = sem filtro.
        """
        self.config = Config()
        self.url = self.config.extract_vars.API_URL
        self.data_inicio = data_inicio
        self.data_termino = data_termino
        self.headers = {
            "Content-Type": self.config.extract_vars.CONTENT_TYPE,
            "User-Agent": self.config.extract_vars.USER_AGENT,
            "Referer": self.config.extract_vars.REFERRER,
            "Origin": self.config.extract_vars.ORIGIN,
            "Accept": self.config.extract_vars.ACCEPT,
            "Accept-Encoding": self.config.extract_vars.ACCEPT_ENCODING,
            "Accept-Language": self.config.extract_vars.ACCEPT_LANGUAGE,
            "Connection": self.config.extract_vars.CONNECTION,
            "Content-Length": self.config.extract_vars.CONTENT_LENGTH,
            "Cookie": self.config.extract_vars.COOKIE,
            "Host": self.config.extract_vars.HOST,
            "Sec-CH-UA": self.config.extract_vars.SEC_CH_UA,
            "Sec-CH-UA-Mobile": self.config.extract_vars.SEC_CH_UA_MOBILE,
            "Sec-CH-UA-Platform": self.config.extract_vars.SEC_CH_UA_PLATFORM,
            "Sec-Fetch-Dest": self.config.extract_vars.SEC_FETCH_DEST,
            "Sec-Fetch-Mode": self.config.extract_vars.SEC_FETCH_MODE,
            "Sec-Fetch-Site": self.config.extract_vars.SEC_FETCH_SITE,
            "X-Requested-With": self.config.extract_vars.X_REQUESTED_WITH
        }
        self.pages = lines // 10
        self.quantity = quantity
        self.all_reports = []
        self.data = {
            'company_name': [],
            'status': [],
            'date': [],
            'report': [],
            'company_response': [],
            'response_date': [],
            'rating_score': [],
            'consumer_written_evaluation': []
        }

    def fetch_reports(self):
        """Makes the request to fetch the reports and parses the HTML."""
        total_reports = 0
        indicePrimeiroResultado = 0

        for page in range(1, self.pages + 1):
            # Parâmetros aceitos pela API: indicePrimeiroResultado, palavrasChave,
            # segmentoMercado, fornecedor, regiao, area, assunto, dataInicio, dataTermino,
            # avaliacao, nota (application/x-www-form-urlencoded)
            payload = {
                "indicePrimeiroResultado": indicePrimeiroResultado,
                "palavrasChave": "",
                "segmentoMercado": "",
                "fornecedor": "",
                "regiao": "",
                "area": "",
                "assunto": "",
                "dataInicio": self.data_inicio,
                "dataTermino": self.data_termino,
                "avaliacao": "",
                "nota": "",
            }
            
            response = requests.post(self.url, headers=self.headers, data=payload)
            
            logger.info(f"Response from page {page}: {response.status_code}")

            if response.status_code == 200:
                try:
                    reports = response.text
                    reports_html = BeautifulSoup(reports, 'html.parser')
                    reports_on_page = reports_html.find_all('div', class_=self.config.extract_vars.CARD_CLASS)
                    
                    if not reports_on_page:
                        logger.info(f"No reports found on page {page}. Stopping.")
                        break
                    
                    self.all_reports.extend(reports_on_page)
                    logger.info(f"Page {page} OK - {len(reports_on_page)} reports found - Total: {total_reports + len(reports_on_page)}")
                    total_reports += len(reports_on_page)
                    
                    indicePrimeiroResultado += 10
                    
                except Exception as e:
                    logger.error(f"Error reading page {page}: {e}")
                    break
            else:
                logger.error(f"Error on page {page}: {response.status_code}")
                break

    def scrap(self):
        """Performs the scraping of the reports from the extracted data."""
        for report in self.all_reports:
            # Company name
            company_name = report.find('h3', class_='relatos-nome-empresa')
            self.data['company_name'].append(company_name.get_text(strip=True) if company_name else None)
            # Status
            report_status = report.find('h4', class_='relatos-status')
            self.data['status'].append(report_status.get_text(strip=True) if report_status else None)
            # Report Date
            report_date = report.find('span', class_='relatos-data')
            self.data['date'].append(report_date.get_text(strip=True) if report_date else None)
            # Report content
            report_text = report.find('p', style='word-wrap: break-word;')
            self.data['report'].append(report_text.get_text(strip=True) if report_text else None)
            # Company response
            company_response = report.find_all('strong', string='Resposta')
            response_text = company_response[0].find_next('p') if company_response else None
            self.data['company_response'].append(response_text.get_text(strip=True) if response_text else None)
            # Response date
            response_date = report.find_all('span', class_='relatos-data')
            self.data['response_date'].append(response_date[1].get_text(strip=True) if len(response_date) > 1 else None)
            # Rating score
            rating = report.find('strong', string='Avaliação')
            rating_score = rating.find_next('p') if rating else None
            self.data['rating_score'].append(rating_score.get_text(strip=True) if rating_score else None)
            # Consumer written evaluation
            all_p = report.find_all('p')
            evaluation = all_p[3] if len(all_p) > 3 else None
            self.data['consumer_written_evaluation'].append(
                evaluation.get_text(strip=True) if evaluation else None
            )

    def get_dataframe(self) -> pd.DataFrame:
        """Converts the extracted data into a pandas DataFrame.
        
        Returns:
            pd.DataFrame: DataFrame containing the extracted data.
        """
        return pd.DataFrame(self.data)

    def save_data(self, data_dir: str, filename: str, format: str = 'csv'):
        """Save the extracted data to a file in the specified format.

        Args:
            data_dir (str): Data files directory
            filename (str): File name
            format (str, optional): File format. Defaults to 'csv'.

        Raises:
            FileNotFoundError: Data directory does not exist.
            ValueError: Unsupported file format.
        """
        full_path = path.join(data_dir, f"{filename}.{format}")
        
        logger.info(f"Saving data to staging area in {format} format")
        logger.info(f"Checking if {data_dir} exists")
        if not path.exists(data_dir):
            raise FileNotFoundError(f"Staging Area directory {data_dir} does not exist.")
        
        dataframe = self.get_dataframe()
        # Check if dataframe file exists
        logger.info(f"Checking if {full_path} exists")
        if path.exists(full_path):
            current_dataframe = pd.read_csv(full_path, encoding="utf-8-sig", sep='|')
            dataframe = pd.concat([current_dataframe, dataframe], ignore_index=True)
            dataframe.drop_duplicates(keep='last', inplace=True)
            
        logger.info(f"Saving data to Staging Area in {format} format")
        try:
            if format == 'csv':
                dataframe.to_csv(full_path, sep='|', index=False, encoding="utf-8-sig")
            elif format == 'json':
                dataframe.to_json(full_path, orient='records', lines=True, force_ascii=False)
            elif format == 'xlsx':
                dataframe.to_excel(full_path, index=False)
            else:
                raise ValueError("Unsupported format. Use 'csv', 'json', or 'xlsx'.")
            logger.info(f"Data saved successfully to Staging Area in {format} format")
        except Exception as e:
            logger.error(f"Error saving the data: {e}")
            raise
