import logging
import os
import platform
import shutil
from subprocess import Popen

logger = logging.getLogger()


def create_auth_files():
    # script to set credentials from
    # https://disc.gsfc.nasa.gov/information/howto?title=How%20to%20Generate%20Earthdata%20Prerequisite%20Files
    IMERG_USERNAME = os.getenv("IMERG_USERNAME")
    IMERG_PASSWORD = os.getenv("IMERG_PASSWORD")

    urs = "urs.earthdata.nasa.gov"  # Earthdata URL to call for authentication

    homeDir = os.path.expanduser("~") + os.sep

    with open(homeDir + ".netrc", "w") as file:
        file.write(
            "machine {} login {} password {}".format(
                urs, IMERG_USERNAME, IMERG_PASSWORD
            )
        )
        file.close()
    with open(homeDir + ".urs_cookies", "w") as file:
        file.write("")
        file.close()
    with open(homeDir + ".dodsrc", "w") as file:
        file.write("HTTP.COOKIEJAR={}.urs_cookies\n".format(homeDir))
        file.write("HTTP.NETRC={}.netrc".format(homeDir))
        file.close()

    logger.info("Saved .netrc, .urs_cookies, and .dodsrc to:", homeDir)

    # Set appropriate permissions for Linux/macOS
    if platform.system() != "Windows":
        Popen("chmod og-rw ~/.netrc", shell=True)
    else:
        # Copy dodsrc to working directory in Windows
        shutil.copy2(homeDir + ".dodsrc", os.getcwd())
        logger.info("Copied .dodsrc to:", os.getcwd())
