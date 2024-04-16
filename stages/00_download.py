from bs4 import BeautifulSoup
import requests
import re
import shutil
from pathlib import Path

# create download directory
downloads = Path("download")
downloads.mkdir()


biogrid_downloads = requests.get("https://downloads.thebiogrid.org/BioGRID/Release-Archive/BIOGRID-4.4.232/",
                                 headers={"accept": "text/html"})
html = biogrid_downloads.text
soup = BeautifulSoup(html)
download_links = soup.find_all("a", {"href": re.compile("https://.*\/Download\/(?!.*OSPREY).*\.tab3.zip$")})
get_link_text = lambda el: el['href']
link_texts = map(get_link_text, download_links)

for link in link_texts:
    response = requests.get(link, stream=True).raw
    outfile_path = downloads / Path(link.split("/")[-1]).name
    print(outfile_path)
    with open(outfile_path, "wb") as f:
        shutil.copyfileobj(response, f)