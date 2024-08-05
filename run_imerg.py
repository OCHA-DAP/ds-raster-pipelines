import logging
from datetime import datetime

from src.imerg.download_imerg import download_imerg
from src.utils.imerg_utils import create_auth_files

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    create_auth_files()

    start_date = datetime.strptime("2000-06-01", '%Y-%m-%d')
    end_date = datetime.strptime("2024-07-01", '%Y-%m-%d')
    download_imerg(start_date=start_date,
                   end_date=end_date,
                   save_raw=True)
