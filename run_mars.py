from mars.download_seas5_tprate import download_seas5
from constants import BBOX_STR_GLOBAL, BBOX_STR_TEST


if __name__ == "__main__":
    START_YEAR = 1981
    END_YEAR = 1982
    download_seas5(START_YEAR, END_YEAR, BBOX_STR_TEST)
