import tempfile

from constants import BBOX_STR_GLOBAL, BBOX_STR_TEST
from mars.download_archive_seas5_tprate import *

if __name__ == "__main__":
    START_YEAR = 1981
    END_YEAR = 1982

    with tempfile.TemporaryDirectory() as td:

        for year in range(START_YEAR, END_YEAR):
            tp_raw = download_archive(year, BBOX_STR_TEST, td)
            process_archive(tp_raw, td)
