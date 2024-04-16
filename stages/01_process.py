import logging
import numpy as np
import pandas as pd
from pathlib import Path
import pyarrow.lib as pa
import zipfile

logging.basicConfig(format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

logger.info("Running process script.")


def create_unzip_dir():
    logger.info("creating unzip/ directory")
    unzip_dir = Path("unzip")
    unzip_dir.mkdir(exist_ok=True)
    return unzip_dir


def unzip_file(f):
    file_path = Path(f)
    logger.info("unzipping file at %s", file_path)
    with open(file_path, "rb") as zip:
        try:
            zip_root = zipfile.ZipFile(zip)
            zip_root.extractall(Path("unzip"))
        except zipfile.BadZipFile:
            logger.debug("Problem unzipping file", exc_info=1)


def read_file(f):
    logger.info("reading CSV from %s", f.name)
    dtypes = {"Entrez Gene Interactor A": np.int64,
              "Entrez Gene Interactor B": np.int64}
    df = pd.read_csv(f, delimiter="\t", dtype=dtypes, low_memory=False)
    df.rename(columns={"#BioGRID Interaction ID": "BioGRID Interaction ID"},
              inplace=True)
    return df


def create_out_dir():
    logger.info("Creating brick/ directory")
    brick_dir = Path("brick")
    brick_dir.mkdir(exist_ok=True)


def create_parquet_file(f: Path):
    df = None
    try:
        df = read_file(f)
    except Exception:
        logger.debug(f"error reading file: {f.name}")
    if df is not None:
        outfile_name = f.relative_to(Path("unzip")).with_suffix(".parquet")
        outfile_path = Path("brick") / outfile_name
        try:
            logger.info("Writing CSV data to Parquet")
            df.to_parquet(outfile_path)
        except pa.ArrowTypeError:
            logger.debug("Error writing Parquet file", exc_info=1)


def run_script():
    create_unzip_dir()
    create_out_dir()
    raw_path = Path("download")
    unzip_path = Path("unzip")
    for file in raw_path.rglob("*.zip"):
        unzip_file(file)
    for file in unzip_path.rglob("*.txt"):
        create_parquet_file(file)


if __name__ == '__main__':
    run_script()
