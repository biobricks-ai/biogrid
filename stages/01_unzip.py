from collections import defaultdict
import logging
from pathlib import Path
import re
import yaml
import zipfile


logging.basicConfig(format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel("INFO")


def create_unzip_dir():
    logger.info("creating unzip/ directory")
    unzip_dir = Path("unzip")
    unzip_dir.mkdir(exist_ok=True)
    return unzip_dir


def create_zip_re_pattern():
    config = None
    with open("version.yaml", "r") as f:
        config = yaml.load(f, yaml.Loader)
    VERSION_STR = re.escape(config['version'])
    ZIP_DIR_PATTERN = re.compile(f".*((?=\.tab3)|(?=\.ptm)|(?=\.chemtab)|(?={VERSION_STR}))")
    return ZIP_DIR_PATTERN


ZIP_DIR_PATTERN = create_zip_re_pattern()
FILE_PATTERN = re.compile(r"(?P<filename>BIOGRID-[A-Z]+)-*(?P<sub_project>[A-Za-z_]*)-{1}(?P<version_no>[0-9][1-9.]*)")

def unzip_file(f):
    file_path = Path(f)
    match = re.search(ZIP_DIR_PATTERN, file_path.stem)
    print(match)
    if match is not None:
        directory_base = match[0]
        filename = re.search(FILE_PATTERN, directory_base)
        if filename is not None:
            groups = filename.groupdict()
            backup = defaultdict(lambda: "")
            backup |= groups
            unzip_path = Path("unzip", backup['filename'] + backup['sub_project'])
            logger.info("unzipping file at %s", unzip_path)
            with open(file_path, "rb") as zip:
                try:
                    zip_root = zipfile.ZipFile(zip)
                    zip_root.extractall(unzip_path)
                except zipfile.BadZipFile:
                    logger.debug("Problem unzipping file", exc_info=1)


                    
def run_script():
    create_unzip_dir()
    raw_path = Path("download")
    for file in raw_path.rglob("*.zip"):
        unzip_file(file)
        

if __name__ == '__main__':
    run_script()