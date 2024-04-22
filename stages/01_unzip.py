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
    return (VERSION_STR, ZIP_DIR_PATTERN)


def unzip_file(f):
    file_path = Path(f)
    version_str, zip_pattern = create_zip_re_pattern()
    match = re.match(zip_pattern, file_path.stem)
    if match is not None:
        directory_base = match[0]
        wo_version_str = re.match(f"^.*(?={version_str})", directory_base)
        unzip_path = Path("unzip")
        if wo_version_str is not None:
            base_path = unzip_path / Path(wo_version_str[0][:-1])
            if not base_path.exists():
                base_path.mkdir()
                logger.info("unzipping file at %s", base_path)
            with open(file_path, "rb") as zip:
                try:
                    zip_root = zipfile.ZipFile(zip)
                    zip_root.extractall(base_path)
                except zipfile.BadZipFile:
                    logger.debug("Problem unzipping file", exc_info=1)

                    
def run_script():
    create_unzip_dir()
    raw_path = Path("download")
    for file in raw_path.rglob("*.zip"):
        unzip_file(file)
        

if __name__ == '__main__':
    run_script()