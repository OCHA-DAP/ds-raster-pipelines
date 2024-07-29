import sys
import tempfile

from constants import BBOX_STR_GLOBAL, BBOX_STR_TEST
from seas5.download_archive_seas5_tprate import *


if __name__ == "__main__":

    if len(sys.argv) < 4:
        print("Usage: python run_seas5.py <scope> <start_year> <end_year>")
        sys.exit(1)

    args = sys.argv
    scope = args[1]
    start_year = int(args[2])
    end_year = int(args[3])

    if (start_year < 1981) or (start_year > 2022):
        print("Error: start_year must be between 1981 and 2022")
        sys.exit(1)

    if (end_year < 1981) or (end_year > 2022):
        print("Error: end_year must be between 1981 and 2022")
        sys.exit(1)
    

    bbox = BBOX_STR_GLOBAL if scope == "global" else BBOX_STR_TEST

    print(f"Running SEAS5 update from {start_year} to {end_year} with {scope} scope")

    with tempfile.TemporaryDirectory() as td:

        for year in range(start_year, end_year):
            tp_raw = download_archive(year, bbox, td)
            process_archive(tp_raw, td)
