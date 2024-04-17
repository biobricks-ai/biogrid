from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from pathlib import Path
import re
import requests
import shutil

logging.basicConfig(format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def create_download_dir():
    logger.info("Creating download/ directory")
    downloads = Path("download")
    downloads.mkdir(exist_ok=True)


URL = "https://downloads.thebiogrid.org/BioGRID/Release-Archive/BIOGRID-4.4.232/"


def get_biogrid_html():
    logger.info("Fetching BioGRID HTML page")
    biogrid_downloads = requests.get(URL, headers={"accept": "text/html"})
    html = biogrid_downloads.text
    soup = BeautifulSoup(html, features="html.parser")
    return soup


tab3_files = re.compile("https://.*/Download/(?!.*OSPREY).*\.tab3.zip$")


def find_download_links():
    download_links = get_biogrid_html().find_all("a", {"href": tab3_files})
    link_texts = map(lambda el: el['href'], download_links)
    return link_texts


def _download_file(url):
    response = requests.get(url, stream=True).raw
    outfile_path = Path("download") / Path(url.split("/")[-1]).name
    logger.info("Downloading file to %s", outfile_path)
    try:
        with open(outfile_path, "wb") as f:
            shutil.copyfileobj(response, f)
            return True
    except OSError:
        return False


def download_files():
    with ThreadPoolExecutor(5, "biogrid_download") as exec:
        future_to_url = {exec.submit(_download_file, url):
                         url for url in find_download_links()}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                if future.result():
                    logger.info("Successfully downloaded file")
            except Exception as exc:
                logger.error('%r generated an exception: %s' % (url, exc))


if __name__ == '__main__':
    logger.info("Running download script")
    create_download_dir()
    download_files()
