from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from pathlib import Path
import re
import requests
import shutil
from yaml import dump

logging.basicConfig(format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def create_config_file(version: str):
    yaml_path = Path("version.yaml")
    yaml_path.touch()
    with open(yaml_path, "w") as yaml:
        dump({'version': version}, yaml)
        

def create_download_dir():
    logger.info("Creating download/ directory")
    downloads = Path("download")
    downloads.mkdir(exist_ok=True)


def find_current_release():
    downloads_page = requests.get("https://downloads.thebiogrid.org/BioGRID").text
    soup = BeautifulSoup(downloads_page, "html.parser")
    return soup.find("a", string="Current-Release")['href']

URL = find_current_release()
VERSION = re.search(r'BIOGRID-([1-9][0-9.]*)', URL).group(1)

if VERSION is not None:
    create_config_file(VERSION)

def get_biogrid_html():
    logger.info("Fetching BioGRID HTML page")
    biogrid_downloads = requests.get(URL, headers={"accept": "text/html"})
    html = biogrid_downloads.text
    soup = BeautifulSoup(html, features="html.parser")
    return soup

REGEX = f"^https://.*/Download/.*/BIOGRID-((?!OSPREY).*\.*(tab3|chemtab|ptm|-{VERSION})|(IDENTIFIERS-{VERSION}.tab)).zip$"
files = re.compile(REGEX)


def find_download_links():
    download_links = get_biogrid_html().find_all("a", {"href": files})
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


def run_script():
    logger.info("Running download script")
    create_download_dir()
    download_files()


if __name__ == '__main__':
    run_script()    
