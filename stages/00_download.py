from bs4 import BeautifulSoup
from pathlib import Path
import re
import requests
import shutil


def create_download_dir():
    downloads = Path("download")
    downloads.mkdir(exist_ok=True)


URL = "https://downloads.thebiogrid.org/BioGRID/Release-Archive/BIOGRID-4.4.232/"


def get_biogrid_html():
    biogrid_downloads = requests.get(URL, headers={"accept": "text/html"})
    html = biogrid_downloads.text
    soup = BeautifulSoup(html)
    return soup


files_regex = re.compile("https://.*/Download/(?!.*OSPREY).*\.tab3.zip$")


def find_download_links():
    download_links = get_biogrid_html().find_all("a", {"href": files_regex})
    link_texts = map(lambda el: el['href'], download_links)
    return link_texts


def download_files():
    for link in find_download_links():
        response = requests.get(link, stream=True).raw
        outfile_path = Path("download") / Path(link.split("/")[-1]).name
        print(outfile_path)
        with open(outfile_path, "wb") as f:
            shutil.copyfileobj(response, f)


if __name__ == '__main__':
    create_download_dir()
    download_files()
