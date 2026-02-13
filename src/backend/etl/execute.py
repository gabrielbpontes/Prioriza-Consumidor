import logging
import os
import sys

# Add project root to path so "src" package can be resolved when running this script directly
_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)

from src.config import Config
from extract import Extract

config = Config()

# Logger configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def execute_data_extraction(
    test: bool = False,
    lines: int = 50_000,
    data_inicio: str = "",
    data_termino: str = "",
):
    """Executa a extração de reclamações do Consumidor.gov.br.

    Args:
        test: se True, extrai 100k linhas e salva em arquivo com sufixo -100_000.
        lines: quantidade de reclamações a extrair (paginação de 10 em 10).
        data_inicio: filtro de data inicial (ex: "01/06/2025"). Vazio = sem filtro.
        data_termino: filtro de data final (ex: "31/07/2025"). Vazio = sem filtro.
    """
    file = config.env_vars.data_file

    if test:
        lines = 100_000
        file += "-100_000"

    try:
        logger.info("Starting the data extraction process.")
        extractor = Extract(
            lines=lines,
            quantity=10,
            data_inicio=data_inicio,
            data_termino=data_termino,
        )
        # Making the request and extracting the reports
        logger.info("Fetching reports...")
        extractor.fetch_reports()
        # Scraping the extracted reports
        logger.info("Starting the scraping of the reports...")
        extractor.scrap()
        # Saving the extracted data to a file
        staging_area = os.path.join(config.project_dir, config.env_vars.data_dir, config.env_vars.staging_dir)
        if not os.path.exists(staging_area):
            logger.info(f"Creating Staging Area at {staging_area}...")
            os.makedirs(staging_area)

        logger.info(f"Saving data to Staging Area {staging_area}...")
        extractor.save_data(staging_area, file, format='csv')

        logger.info("Data extraction process completed successfully!")

    except Exception as e:
        logger.error(f"An error occurred during the extraction process: {e}")
        extractor.save_data(staging_area, file, format='csv')

if __name__ == "__main__":
    # Para um período específico use data_inicio e data_termino (formato dependente da API).
    execute_data_extraction(
        test=False,
        lines=200,
        data_inicio="01-06-2025",
        data_termino="31-07-2025",
    )